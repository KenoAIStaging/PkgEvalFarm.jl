# Run/job lifecycle against DynamoDB (state), SQS (dispatch) and S3 (logs).
#
# SQS is at-least-once, so every transition is guarded by a conditional write in
# DynamoDB: a job that is already in a terminal state is never re-run, its stray
# message is simply deleted.

# how long a claimed message stays invisible; workers heartbeat well within this
const VISIBILITY_TIMEOUT = 30 * 60
const HEARTBEAT_INTERVAL = 5 * 60
# ~3 hours: beyond any legitimate job (PkgEval's own limit is 45 min, doubled for
# slow packages), so a hung worker eventually lets its job return to the queue
const MAX_HEARTBEATS = 36
# redelivery delay while waiting for a requested CI build (~30-40 min builds;
# see the raised maxReceiveCount on the queues, which must cover build_time/delay)
const BUILD_RETRY_DELAY = 10 * 60

isodate(t=Dates.now(UTC)) = Dates.format(t, dateformat"yyyy-mm-dd\THH:MM:SS\Z")

# retries transient faults (throttling, TransactionConflict, network); a
# conditional failure is a deterministic answer, not a fault — fail fast so
# dedup/lost-race paths see it immediately
aws_retry(f; n=5) = retry(f; delays=ExponentialBackOff(; n, first_delay=1, max_delay=30),
                          check=(s, e) -> !is_conditional_failure(e))()

# real AWS uses the ConditionalCheckFailedException code; some emulators only carry
# the human-readable message with a generic 400 code. A cancelled transaction
# reports TransactionCanceledException with the per-item reasons in the message.
is_conditional_failure(err) = err isa AWS.AWSException &&
    (occursin("ConditionalCheckFailed", err.code) ||
     occursin("TransactionCanceled", err.code) && occursin("ConditionalCheckFailed", err.message) ||
     occursin("conditional request failed", lowercase(err.message)))

# S3's answer to an If-None-Match: * PUT when the key already exists
is_precondition_failed(err) = err isa AWS.AWSException &&
    (err.code == "PreconditionFailed" || err.code == "412")


## submission

"""
    create_run(ctx, spec::RunSpec; submitter, run_id=new_run_id()) -> run_id

Create the run in DynamoDB (one `PutItem`) and enqueue a single *expand* message.
Job fan-out always happens on a worker via [`expand_run`](@ref): for an explicit
package list (stored on the run item) that is a mere formality, and for an empty one
("all packages") the worker computes the selection — which may require the Julia
build under test, so it cannot happen on submitters. Keeping submission this small
also lets the bot Lambda submit runs without an AWS SDK.
"""
function create_run(ctx::FarmCtx, spec::RunSpec; submitter::AbstractString,
                    run_id::AbstractString=new_run_id(), reuse::Bool=true)
    config_names = [cfg.name for cfg in spec.configs]
    allunique(config_names) || error("configuration names must be unique: $config_names")
    SEAL_CONFIG_NAME in config_names &&
        error("configuration name \"$SEAL_CONFIG_NAME\" is reserved for seal jobs")

    Dynamodb.put_item(ddb_item(Dict(
            "run_id" => run_id,
            "created_at" => isodate(),
            "submitter" => String(submitter),
            "status" => "expanding",
            "configs" => JSON.json(config_to_dict.(spec.configs)),
            "packages" => JSON.json(spec.packages),  # empty = all compatible packages
            "context" => JSON.json(spec.context),
            "total_jobs" => 0,
            "completed_jobs" => 0,
            # baseline reuse (expand_run); false = the submitter wants a fresh
            # evaluation of the against side even if matching results exist
            "reuse" => reuse,
        )), ctx.cfg.runs_table,
        Dict("ConditionExpression" => "attribute_not_exists(run_id)");
        aws_config=ctx.aws)

    aws_retry() do
        SQS.send_message(JSON.json(Dict("run_id" => run_id, "expand" => true)),
                         slow_queue(ctx.cfg); aws_config=ctx.aws)
    end
    return run_id
end

# packages with no history are estimated at PkgEval's default time limit: the
# conservative direction, since an unknown straggler scheduled early costs
# nothing while one scheduled late costs the whole tail
const DEFAULT_DURATION_ESTIMATE = 45.0 * 60

# fallback fleet slot capacity for the cutoff computation, used when neither
# the PKGEVAL_FLEET_SLOTS override nor a live ASG answer is available (see
# live_fleet_slots in worker.jl). Overestimating pushes the cutoff up (more
# jobs classed slow) — again the conservative direction.
const DEFAULT_FLEET_SLOTS = 128
fleet_slots() = parse(Int, get(ENV, "PKGEVAL_FLEET_SLOTS", string(DEFAULT_FLEET_SLOTS)))

"Completed runs, newest first, as `(created_at, run_id, configs, total_jobs)` tuples."
function completed_runs(ctx::FarmCtx)
    runs = Tuple{String,String,Any,Int}[]
    start_key = nothing
    while true
        params = Dict{String,Any}(
            "FilterExpression" => "#s = :done",
            "ExpressionAttributeNames" => Dict("#s" => "status"),
            "ExpressionAttributeValues" => ddb_item(Dict(":done" => "done")),
            "ProjectionExpression" => "run_id, created_at, configs, total_jobs")
        start_key === nothing || (params["ExclusiveStartKey"] = start_key)
        resp = aws_retry() do
            Dynamodb.scan(ctx.cfg.runs_table, params; aws_config=ctx.aws)
        end
        for item in ddb_parse.(resp["Items"])
            # seal runs are `done` runs too, and their config differs from a
            # user config only by `name` — which config_fingerprint ignores —
            # so without this they would donate "sealed" statuses as baseline
            # results (and pollute duration estimates)
            startswith(String(item["run_id"]), "seal-") && continue
            push!(runs, (String(get(item, "created_at", "")), String(item["run_id"]),
                         JSON.parse(item["configs"]), Int(get(item, "total_jobs", 0))))
        end
        start_key = get(resp, "LastEvaluatedKey", nothing)
        start_key === nothing && break
    end
    return sort!(runs; rev=true)
end

"""
Per-package duration estimates from recent completed runs (up to `max_donors`
of them). Duration is mostly package-intrinsic, so estimates transfer across
Julia versions where results would not; the max over configs and donors is
used, since underestimating is what causes stragglers.

Donors big enough to plausibly cover the request are preferred over merely
recent ones: recency alone lets a streak of small runs crowd every
ecosystem-scale donor out of the window, leaving the whole request on the
default estimate — which un-front-loads exactly the jobs the slow queue
exists for (seen live: LinearAlgebra unestimated, claimed dead last, an
11-hour idle tail on runs gh-51739*).
"""
function duration_estimates(ctx::FarmCtx, packages::Vector{String},
                            donors::Vector{<:Tuple}; max_donors::Int=3)
    est = Dict{String,Float64}()
    wanted = Set(packages)
    covering(d) = d[4] >= length(wanted)   # total_jobs spans ≥ one config's worth
    ordered = vcat(filter(covering, donors), filter(!covering, donors))
    for (_, donor_id) in first(ordered, max_donors)
        length(est) == length(wanted) && break
        for job in run_jobs(ctx, donor_id)
            pkg = String(job["package"])
            pkg in wanted || continue
            get(job, "status", "") in TERMINAL_STATUSES || continue
            get(job, "status", "") == "error" && continue
            duration = Float64(get(job, "duration", 0.0))
            duration > 0 || continue
            est[pkg] = max(get(est, pkg, 0.0), duration)
        end
    end
    return est
end

"""
Duration above which a job goes to the slow queue, chosen from the run's own
mix: the smallest cutoff such that the fast class still holds `margin` × (the
longest estimated job) × (fleet slot capacity) of aggregate work. The cutoff
directly bounds the end-of-run tail (once only the fast queue remains, no
machine is extended by more than one fast job), while the backfill condition
guarantees every slow job — claimed in whatever order SQS feels like — starts
with enough short work queued behind it. Runs too small to satisfy the
condition get `Inf`: a single class, since there is nothing to backfill with.
"""
function duration_cutoff(estimates::Vector{Float64}; slots::Int=fleet_slots(),
                         margin::Float64=2.0)
    isempty(estimates) && return Inf
    need = margin * maximum(estimates) * slots
    acc = 0.0
    for d in sort(estimates)
        acc += d
        acc >= need && return d
    end
    return Inf
end

"""
Find reusable baseline results for a run: the `against` config's jobs, taken
from the most recent `done` run containing a config with the same content
fingerprint. Reuse is sound only when the julia spec is immutable (exact sha or
release tag) — the bot pins branch specs to shas at submission for this reason
— and the submitter can veto it (`reuse = false` / `--fresh-baseline`) when the
existing baseline looks flaky.

Returns `(config_name, donor_run_id, Dict(package => donor job item))`, with an
empty dict when there is nothing to reuse. Infrastructure failures ("error")
are never reused; real results (including fail/crash/kill) are.
"""
function baseline_reuse_plan(ctx::FarmCtx, run::AbstractDict, packages::Vector{String},
                             completed::Vector{<:Tuple}=completed_runs(ctx))
    none = ("", "", Dict{String,Dict{String,Any}}())
    get(run, "reuse", true) == true || return none
    i = findfirst(c -> c["name"] == "against", run["configs"])
    i === nothing && return none
    against = run["configs"][i]
    reusable_julia_spec(String(against["julia"])) || return none
    fp = config_fingerprint(against)

    donors = Tuple{String,String,String}[]  # (created_at, run_id, config name)
    for (created_at, run_id, configs) in completed
        run_id == run["run_id"] && continue
        for cfg in configs
            config_fingerprint(cfg) == fp || continue
            push!(donors, (created_at, run_id, String(cfg["name"])))
        end
    end
    isempty(donors) && return none

    wanted = Set(packages)
    for (_, donor_id, donor_cfg) in donors
        results = Dict{String,Dict{String,Any}}()
        for job in run_jobs(ctx, donor_id)
            job["config"] == donor_cfg || continue
            job["package"] in wanted || continue
            status = get(job, "status", "")
            status in TERMINAL_STATUSES && status != "error" || continue
            results[String(job["package"])] = job
        end
        isempty(results) || return ("against", donor_id, results)
    end
    return none
end

"Write already-completed job items carrying a donor's results (batched)."
function write_reused_jobs(ctx::FarmCtx, run_id::AbstractString, donor_id::AbstractString,
                           jobs::Vector{JobRef}, results::AbstractDict)
    now = isodate()
    for batch in Iterators.partition(jobs, 25)
        requests = map(batch) do job
            donor = results[job.package]
            Dict("PutRequest" => Dict("Item" => ddb_item(Dict(
                "run_id" => job.run_id,
                "job_key" => job_key(job),
                "config" => job.config,
                "package" => job.package,
                "status" => donor["status"],
                "reason" => get(donor, "reason", nothing),
                "reason_message" => get(donor, "reason_message", nothing),
                "version" => get(donor, "version", nothing),
                "duration" => get(donor, "duration", 0.0),
                # the donor's log verbatim: log_key is stored per job precisely
                # so a result can point outside its own run's prefix
                "log_key" => get(donor, "log_key", nothing),
                (get(donor, "error_line", nothing) === nothing ? () :
                 (("error_line" => donor["error_line"]),))...,
                "reused_from" => donor_id,
                "finished_at" => now,
                "attempts" => 0))))
        end
        aws_retry() do
            resp = Dynamodb.batch_write_item(Dict(ctx.cfg.jobs_table => collect(requests));
                                             aws_config=ctx.aws)
            unprocessed = get(resp, "UnprocessedItems", Dict())
            isempty(unprocessed) || error("unprocessed DynamoDB writes; retrying")
        end
    end
end

"Create the DynamoDB job items (25 per BatchWriteItem), idempotently."
function write_jobs(ctx::FarmCtx, jobs::Vector{JobRef}; est=Returns(nothing))
    for batch in Iterators.partition(jobs, 25)
        requests = [Dict("PutRequest" => Dict("Item" => ddb_item(Dict(
                        "run_id" => job.run_id,
                        "job_key" => job_key(job),
                        "config" => job.config,
                        "package" => job.package,
                        "status" => "pending",
                        "attempts" => 0,
                        # the scheduler's duration estimate, stored so the
                        # bot's ETA can weigh remaining work by expected cost
                        # instead of assuming all jobs are equal
                        (est(job) === nothing ? () : ("est" => est(job),))...,
                    )))) for job in batch]
        aws_retry() do
            resp = Dynamodb.batch_write_item(Dict(ctx.cfg.jobs_table => requests);
                                             aws_config=ctx.aws)
            unprocessed = get(resp, "UnprocessedItems", Dict())
            isempty(unprocessed) || error("unprocessed DynamoDB writes; retrying")
        end
    end
end

"""
    expand_run(ctx, run_id, packages) -> njobs

Worker-side completion of an `expanding` run: write the job items, flip the run to
`active` with the real job count, and enqueue the job messages.

Safe under at-least-once delivery: job items are written before the conditional
status flip, so a crashed expansion is simply redone, and because duplicate job
messages are harmless, a redelivered expand message after the flip just re-enqueues.
"""
function expand_run(ctx::FarmCtx, run_id::AbstractString, packages::Vector{String})
    run = get_run(ctx, run_id)
    # a stray expand message for a finished run must not re-enqueue its jobs
    run["status"] == "done" && return run["total_jobs"]
    config_names = [String(c["name"]) for c in run["configs"]]
    jobs = [JobRef(run_id, cfg, pkg) for cfg in config_names for pkg in packages]
    isempty(jobs) && error("expansion of $run_id produced no jobs")

    completed = completed_runs(ctx)

    # baseline reuse: against-side jobs with a matching prior result are written
    # pre-completed (pointing at the donor's log) and never enqueued
    reuse_cfg, donor_id, reused_results = baseline_reuse_plan(ctx, run, packages, completed)
    reused = [j for j in jobs if j.config == reuse_cfg && haskey(reused_results, j.package)]
    fresh = setdiff(jobs, reused)
    isempty(reused) || @info "reusing baseline results" run_id donor_id n=length(reused)

    # straggler avoidance: jobs above the (run-mix-derived) duration cutoff go
    # to the slow queue, which workers drain first
    est = duration_estimates(ctx, [j.package for j in fresh], completed)
    job_est(j) = get(est, j.package, DEFAULT_DURATION_ESTIMATE)
    cutoff = duration_cutoff([job_est(j) for j in fresh]; slots=live_fleet_slots(ctx))
    is_slow(j) = job_est(j) > cutoff

    # don't rewrite (= reset) job items once the run went active — after that point a
    # redelivered expand message only needs to make sure the messages went out
    if run["status"] == "expanding"
        write_jobs(ctx, fresh; est=job_est)
        isempty(reused) || write_reused_jobs(ctx, run_id, donor_id, reused, reused_results)
    end

    # sealing (docs/sealing.md): idempotent, so a redelivered expand message
    # re-running it only re-sends harmless duplicate seal messages. Must happen
    # before test messages go out — workers gate test jobs on the mapping below
    seal_runs = setup_sealing(ctx, run, fresh)
    if seal_runs !== nothing
        Dynamodb.update_item(ddb_item(Dict("run_id" => run_id)), ctx.cfg.runs_table,
            Dict("UpdateExpression" => "SET seal_runs = :sr",
                 "ExpressionAttributeValues" => ddb_item(Dict(":sr" => seal_runs)));
            aws_config=ctx.aws)
    end

    try
        Dynamodb.update_item(ddb_item(Dict("run_id" => run_id)), ctx.cfg.runs_table,
            Dict("ConditionExpression" => "#s = :expanding",
                 # expansion succeeding also retires any waiting-for-build note
                 "UpdateExpression" => "SET #s = :active, total_jobs = :total, " *
                                       "completed_jobs = :reused, updated_at = :now " *
                                       "REMOVE #w",
                 "ExpressionAttributeNames" => Dict("#s" => "status", "#w" => "waiting"),
                 "ExpressionAttributeValues" => ddb_item(Dict(
                     ":expanding" => "expanding", ":active" => "active",
                     ":total" => length(jobs), ":reused" => length(reused),
                     ":now" => isodate())));
            aws_config=ctx.aws)
    catch err
        if is_conditional_failure(err)
            # already expanded by an earlier (interrupted) attempt; messages may or
            # may not have been sent, so fall through and (re-)enqueue everything —
            # claim_job drops messages for jobs that are already completed (which
            # covers the reused ones), so over-enqueueing is merely a little churn
            fresh = jobs
        else
            rethrow()
        end
    end
    slow_jobs = [j for j in fresh if is_slow(j)]
    fast_jobs = [j for j in fresh if !is_slow(j)]
    isempty(slow_jobs) ||
        @info "routing long jobs to the slow queue" run_id nslow=length(slow_jobs) cutoff
    enqueue_jobs(ctx, slow_jobs; queue_url=slow_queue(ctx.cfg))
    enqueue_jobs(ctx, fast_jobs)
    return length(jobs)
end

"Enqueue SQS messages for jobs (also used to re-drive stalled jobs)."
function enqueue_jobs(ctx::FarmCtx, jobs::Vector{JobRef};
                      queue_url::AbstractString=ctx.cfg.queue_url)
    for batch in Iterators.partition(jobs, 10)
        entries = [Dict("Id" => string(i), "MessageBody" => json_message(job))
                   for (i, job) in enumerate(batch)]
        aws_retry() do
            resp = SQS.send_message_batch(entries, queue_url; aws_config=ctx.aws)
            failed = get(resp, "Failed", nothing)
            failed === nothing || isempty(failed) || error("failed to enqueue $(length(failed)) messages; retrying")
        end
    end
end


## claiming and completing (worker side)

struct ClaimedJob
    job::JobRef
    receipt_handle::String
    attempts::Int
    queue_url::String   # receipt handles are queue-specific
end

"A received *expand* message: the worker should compute and fan out the run's jobs."
struct ClaimedExpand
    run_id::String
    receipt_handle::String
    queue_url::String
end

"""
    claim_job(ctx; wait=20) -> Union{ClaimedJob,Nothing}

Long-poll the queue for one job and transition it to `running`. Returns `nothing` if
the queue was empty or the received job turned out to be already finished (its stray
duplicate message is deleted).
"""
function claim_job(ctx::FarmCtx; wait::Int=20)
    # Seal jobs first, then slow jobs: sealing must run ahead of the tests it
    # warms, and long jobs must start while short work remains to backfill
    # behind them — and queue priority is the only ordering SQS actually
    # honors. WaitTimeSeconds=1 is still a long poll (it consults every
    # storage host, unlike wait=0 which samples and can miss), so an "empty"
    # answer is trustworthy.
    slow = slow_queue(ctx.cfg)
    queues = Tuple{String,Int}[]
    if sealing_enabled(ctx.cfg) && pkgeval_supports_seal()
        # derivations first: nohold leaves, always runnable — never let them
        # sit behind the seal jobs that hold on them (head-of-line inversion)
        dq = deriv_queue(ctx.cfg)
        dq == ctx.cfg.seal_queue_url || push!(queues, (dq, 1))
        push!(queues, (ctx.cfg.seal_queue_url, 1))
    end
    slow == ctx.cfg.queue_url || push!(queues, (slow, 1))
    push!(queues, (ctx.cfg.queue_url, wait))
    return claim_from_queues(ctx, queues)
end

"Claim seal-pipeline work only (derivations first) — the hold-and-fill path
of a gated test job, the donor slots, and the spill receiver."
function claim_seal_job(ctx::FarmCtx; wait::Int=1)
    sealing_enabled(ctx.cfg) || return nothing
    dq = deriv_queue(ctx.cfg)
    queues = dq == ctx.cfg.seal_queue_url ? [(dq, wait)] :
             [(dq, 1), (ctx.cfg.seal_queue_url, wait)]
    return claim_from_queues(ctx, queues)
end

function claim_from_queues(ctx::FarmCtx, queues)
    message, from_queue = nothing, ""
    for (queue, w) in queues
        resp = SQS.receive_message(queue,
            Dict("WaitTimeSeconds" => w, "MaxNumberOfMessages" => 1,
                 "VisibilityTimeout" => VISIBILITY_TIMEOUT,
                 "AttributeNames" => ["ApproximateReceiveCount"]);
            aws_config=ctx.aws)
        messages = get(resp, "Messages", nothing)
        (messages === nothing || isempty(messages)) && continue
        message, from_queue = only(messages), queue
        break
    end
    message === nothing && return nothing
    receipt = message["ReceiptHandle"]
    body = JSON.parse(message["Body"])
    get(body, "expand", false) == true &&
        return ClaimedExpand(body["run_id"], receipt, from_queue)
    job = JobRef(body)
    receive_count = parse(Int, get(get(message, "Attributes", Dict()), "ApproximateReceiveCount", "1"))

    # flip pending -> running. A job already `running` is re-claimable only
    # when its worker looks dead (heartbeat_at stale beyond three beats, or
    # cleared by release_job): a *live* long evaluation must not be
    # re-executed by a stray duplicate message — those bounce off the fresh
    # stamp and are deleted below, exactly like duplicates of finished jobs.
    # A genuinely dead worker's redelivery arrives after the ≥30-minute
    # visibility lapse, by which point the stamp is well past stale.
    try
        Dynamodb.update_item(
            ddb_item(Dict("run_id" => job.run_id, "job_key" => job_key(job))),
            ctx.cfg.jobs_table,
            Dict("ConditionExpression" => "attribute_exists(run_id) AND " *
                     "(#s = :pending OR (#s = :running AND " *
                     "(attribute_not_exists(heartbeat_at) OR heartbeat_at < :stale)))",
                 "UpdateExpression" => "SET #s = :running, worker = :worker, " *
                                       "started_at = :now, heartbeat_at = :now ADD attempts :one",
                 "ExpressionAttributeNames" => Dict("#s" => "status"),
                 "ExpressionAttributeValues" => ddb_item(Dict(
                     ":pending" => "pending", ":running" => "running",
                     ":worker" => worker_identity(), ":now" => isodate(),
                     ":stale" => isodate(Dates.now(Dates.UTC) - Dates.Second(3 * HEARTBEAT_INTERVAL)),
                     ":one" => 1)));
            aws_config=ctx.aws)
    catch err
        if is_conditional_failure(err)
            # already finished, still live on another worker (duplicate
            # delivery), or the run item was deleted
            SQS.delete_message(from_queue, receipt; aws_config=ctx.aws)
            return nothing
        end
        rethrow()
    end
    return ClaimedJob(job, receipt, receive_count, from_queue)
end

worker_identity() = string(get(ENV, "USER", "unknown"), "@", gethostname())

"""
Extend the message visibility while the job is still being evaluated, and
stamp the job item's `heartbeat_at`: claims use the stamp to tell a *live*
running job (a long precompile — stray duplicate messages must bounce off
it, or they re-execute the whole thing) from an *abandoned* one (worker
died; beats stopped; the ≥30-minute redelivery finds the stamp stale and
re-claims). The stamp is best-effort — a missed write only risks one
duplicate execution, which publication and recording already tolerate.
"""
function heartbeat(ctx::FarmCtx, claimed::Union{ClaimedJob,ClaimedExpand};
                   extend::Int=VISIBILITY_TIMEOUT)
    SQS.change_message_visibility(claimed.queue_url, claimed.receipt_handle, extend;
                                  aws_config=ctx.aws)
    claimed isa ClaimedJob || return
    try
        Dynamodb.update_item(
            ddb_item(Dict("run_id" => claimed.job.run_id, "job_key" => job_key(claimed.job))),
            ctx.cfg.jobs_table,
            Dict("ConditionExpression" => "#s = :running",
                 "UpdateExpression" => "SET heartbeat_at = :now",
                 "ExpressionAttributeNames" => Dict("#s" => "status"),
                 "ExpressionAttributeValues" => ddb_item(Dict(
                     ":running" => "running", ":now" => isodate())));
            aws_config=ctx.aws)
    catch err
        is_conditional_failure(err) || @warn "job heartbeat stamp failed" err
    end
    return
end

"""
Terminally fail a run from the worker side (e.g. its Julia build failed) with
a human-readable reason. The bot's poll notices `failed` without `reported`
and notifies the submitter. Conditional on the run still being in flight, so
a concurrent completion or another failure path wins quietly.
"""
function fail_run(ctx::FarmCtx, run_id::AbstractString, why::AbstractString)
    try
        Dynamodb.update_item(
            ddb_item(Dict("run_id" => run_id)),
            ctx.cfg.runs_table,
            Dict("ConditionExpression" => "#s IN (:expanding, :active)",
                 "UpdateExpression" => "SET #s = :failed, finished_at = :now, failure_reason = :why",
                 "ExpressionAttributeNames" => Dict("#s" => "status"),
                 "ExpressionAttributeValues" => ddb_item(Dict(
                     ":expanding" => "expanding", ":active" => "active",
                     ":failed" => "failed", ":now" => isodate(), ":why" => String(why))));
            aws_config=ctx.aws)
        @info "run failed" run_id why
    catch err
        is_conditional_failure(err) || rethrow()
    end
    return nothing
end

"""
Record on the run item which Julia builds its work is stalled on — keyed by
configuration name, so a run whose primary *and* baseline both need building
shows both waits — letting the dashboard explain a run that shows no progress
instead of looking hung. Repeat notes for a config only land when they learn
the Buildkite URL, so the every-10-minutes retry chatter of a whole fleet
degrades to cheap conditional no-ops. Cleared by `expand_run`'s flip to
active and by every `record_result` (a config still stalled simply re-stamps
on its next retry).
"""
function note_run_waiting(ctx::FarmCtx, run_id::AbstractString;
                          config::AbstractString, sha::AbstractString,
                          variant::AbstractString,
                          url::Union{AbstractString,Nothing}=nothing)
    in_flight = Dict(":expanding" => "expanding", ":active" => "active")
    try
        # two writes, because one expression cannot both create a map and set
        # a key inside it: first materialize the (empty) map…
        try
            Dynamodb.update_item(ddb_item(Dict("run_id" => run_id)), ctx.cfg.runs_table,
                Dict("ConditionExpression" => "#s IN (:expanding, :active) AND " *
                                              "attribute_not_exists(#w)",
                     "UpdateExpression" => "SET #w = :empty",
                     "ExpressionAttributeNames" => Dict("#s" => "status", "#w" => "waiting"),
                     "ExpressionAttributeValues" => ddb_item(merge(in_flight,
                         Dict(":empty" => Dict{String,Any}()))));
                aws_config=ctx.aws)
        catch err
            is_conditional_failure(err) || rethrow()   # already there: the common case
        end
        # …then fill this config's slot, skipping writes that change nothing
        Dynamodb.update_item(ddb_item(Dict("run_id" => run_id)), ctx.cfg.runs_table,
            Dict("ConditionExpression" =>
                     "#s IN (:expanding, :active) AND attribute_exists(#w) AND " *
                     "(attribute_not_exists(#w.#c) OR #w.#c.#u <> :url)",
                 "UpdateExpression" => "SET #w.#c = :entry",
                 "ExpressionAttributeNames" => Dict("#s" => "status", "#w" => "waiting",
                                                    "#c" => String(config), "#u" => "url"),
                 "ExpressionAttributeValues" => ddb_item(merge(in_flight, Dict(
                     ":url" => url === nothing ? "" : String(url),
                     ":entry" => Dict("sha" => String(sha), "variant" => String(variant),
                                      "url" => url === nothing ? "" : String(url),
                                      "since" => isodate())))));
            aws_config=ctx.aws)
    catch err
        # a failed condition is the common no-change case, not a problem
        is_conditional_failure(err) || @warn "failed to note run wait state" run_id err
    end
    return nothing
end

"Give up on a claimed job without recording a result; it will be redelivered."
function release_job(ctx::FarmCtx, claimed::Union{ClaimedJob,ClaimedExpand}; delay::Int=60)
    # clear the liveness stamp FIRST: a released message must be immediately
    # re-claimable (drain fast-release, transient-error retries), and a fresh
    # heartbeat_at would make the next claim mistake the job for still-live
    # and delete the very message being released
    if claimed isa ClaimedJob
        try
            Dynamodb.update_item(
                ddb_item(Dict("run_id" => claimed.job.run_id, "job_key" => job_key(claimed.job))),
                ctx.cfg.jobs_table,
                Dict("ConditionExpression" => "#s = :running",
                     "UpdateExpression" => "REMOVE heartbeat_at",
                     "ExpressionAttributeNames" => Dict("#s" => "status"),
                     "ExpressionAttributeValues" => ddb_item(Dict(":running" => "running")));
                aws_config=ctx.aws)
        catch err
            is_conditional_failure(err) || @warn "failed to clear liveness stamp on release" err
        end
    end
    try
        SQS.change_message_visibility(claimed.queue_url, claimed.receipt_handle, delay;
                                      aws_config=ctx.aws)
    catch err
        @warn "failed to release job; it will reappear after the visibility timeout" err
    end
end

"""
    record_result(ctx, claimed, result::JobResult)

Upload the log to S3, mark the job finished in DynamoDB, bump the run's completion
counter (flipping the run to `done` on the last job), and delete the SQS message.
"""
function record_result(ctx::FarmCtx, claimed::ClaimedJob, result::JobResult)
    job = claimed.job
    result.status in TERMINAL_STATUSES || error("not a terminal status: $(result.status)")

    # If-None-Match makes the upload create-only (and the bucket policy
    # *requires* workers to send it): first write wins, a stored log is
    # immutable. A taken key means a dead earlier attempt uploaded its log but
    # died before recording a result — and that attempt may well have ended
    # differently (a time-limit kill whose retry then passed, say), so adopting
    # its log would pair this result with a log that contradicts it. Instead
    # this attempt's log goes under the next free attempt-numbered key, and
    # log_key records wherever it landed: a job's result and its log always
    # describe the same execution.
    key = nothing
    if result.log !== nothing
        for attempt in 1:10
            candidate = log_key(job.run_id, job.config, job.package, attempt)
            created = aws_retry() do
                try
                    S3.put_object(ctx.cfg.bucket, candidate,
                        Dict("body" => result.log,
                             "headers" => Dict("Content-Type" => "text/plain; charset=utf-8",
                                               "If-None-Match" => "*"));
                        aws_config=ctx.aws)
                    true
                catch err
                    is_precondition_failed(err) || rethrow()
                    false
                end
            end
            created && (key = candidate; break)
        end
        key === nothing &&
            @warn "every log key already taken; recording without a log" job.package
    end
    # first meaningful error line of hard failures, so the report can cluster
    # shared failure signatures without the logs; absent on other jobs to keep
    # the items (fetched wholesale by every run_jobs read) small
    line = result.status in ("fail", "crash") && result.log !== nothing ?
           error_line(result.log) : nothing

    # The terminal-status write and the counter bump must be one atomic unit: a
    # worker dying between two separate writes leaves every job terminal but the
    # counter one short — the run can then never reach `done` (happened to run
    # gh-5107486319). The `:running` condition also means a lost race (another
    # worker already recorded this job) cancels the whole transaction, so the
    # counter can't double-count.
    aws_retry() do
        Dynamodb.transact_write_items(
            [Dict("Update" => Dict(
                 "TableName" => ctx.cfg.jobs_table,
                 "Key" => ddb_item(Dict("run_id" => job.run_id, "job_key" => job_key(job))),
                 "ConditionExpression" => "#s = :running",
                 "UpdateExpression" => "SET #s = :status, reason = :reason, " *
                                       "reason_message = :reason_message, version = :version, " *
                                       "#d = :duration, wall = :wall, finished_at = :now, " *
                                       "log_key = :log_key, cost = :cost, peak_rss = :peak_rss" *
                                       (line === nothing ? "" : ", error_line = :error_line"),
                 "ExpressionAttributeNames" => Dict("#s" => "status", "#d" => "duration"),
                 "ExpressionAttributeValues" => ddb_item(Dict(
                     ":running" => "running", ":status" => result.status,
                     ":reason" => result.reason,
                     # store the human-readable description so report generation
                     # doesn't need PkgEval's reason table (the bot Lambda lacks it)
                     ":reason_message" => result.reason === nothing ? nothing :
                                          reason_message(result.reason),
                     ":version" => result.version,
                     ":duration" => result.duration, ":wall" => result.wall,
                     ":now" => isodate(),
                     # what this job's slot-time cost on this worker (see
                     # SLOT_HOURLY_RATE); the report sums these per run. Priced
                     # by wall occupancy when the worker measured it — the
                     # test-phase duration alone misses most of the slot time
                     ":cost" => SLOT_HOURLY_RATE[] === nothing ? nothing :
                                (result.wall > 0 ? result.wall : result.duration) /
                                3600 * something(SLOT_HOURLY_RATE[]) * result.slots,
                     ":peak_rss" => result.peak_rss,
                     ":log_key" => key,
                     (line === nothing ? () : ((":error_line" => line),))...)))),
             Dict("Update" => Dict(
                 "TableName" => ctx.cfg.runs_table,
                 "Key" => ddb_item(Dict("run_id" => job.run_id)),
                 # updated_at is the dashboard's activity clock: when this run
                 # last made progress, as opposed to created_at/finished_at.
                 # Progress also retires any waiting-for-build note (a job
                 # still stalled on a build simply re-stamps it on its retry)
                 "UpdateExpression" => "SET updated_at = :now ADD completed_jobs :one " *
                                       "REMOVE #w",
                 "ExpressionAttributeNames" => Dict("#w" => "waiting"),
                 "ExpressionAttributeValues" => ddb_item(Dict(
                     ":one" => 1, ":now" => isodate()))))];
            aws_config=ctx.aws)
    end

    # the worker finishing the last job marks the run done (transactions can't
    # return values, hence the separate consistent read; the conditional flip
    # tolerates racing workers, and a death right here is healed by the bot's
    # scheduled reconciler)
    resp = aws_retry() do
        Dynamodb.get_item(
            ddb_item(Dict("run_id" => job.run_id)), ctx.cfg.runs_table,
            Dict("ConsistentRead" => true);
            aws_config=ctx.aws)
    end
    attrs = ddb_parse(resp["Item"])
    if attrs["completed_jobs"] >= attrs["total_jobs"] && attrs["status"] == "active"
        Dynamodb.update_item(
            ddb_item(Dict("run_id" => job.run_id)), ctx.cfg.runs_table,
            Dict("ConditionExpression" => "#s = :active AND completed_jobs >= total_jobs",
                 "UpdateExpression" => "SET #s = :done, finished_at = :now",
                 "ExpressionAttributeNames" => Dict("#s" => "status"),
                 "ExpressionAttributeValues" => ddb_item(Dict(
                     ":active" => "active", ":done" => "done", ":now" => isodate())));
            aws_config=ctx.aws)
    end

    SQS.delete_message(claimed.queue_url, claimed.receipt_handle; aws_config=ctx.aws)
    return nothing
end


## inspection (submitter/report side)

function get_run(ctx::FarmCtx, run_id::AbstractString)
    resp = Dynamodb.get_item(ddb_item(Dict("run_id" => run_id)), ctx.cfg.runs_table;
                             aws_config=ctx.aws)
    haskey(resp, "Item") || error("no such run: $run_id")
    run = ddb_parse(resp["Item"])
    run["configs"] = JSON.parse(run["configs"])
    run["context"] = JSON.parse(run["context"])
    run["packages"] = JSON.parse(get(run, "packages", "[]"))
    return run
end

"All job items of a run (paginated DynamoDB query)."
function run_jobs(ctx::FarmCtx, run_id::AbstractString)
    jobs = Dict{String,Any}[]
    start_key = nothing
    while true
        params = Dict{String,Any}(
            "KeyConditionExpression" => "run_id = :run_id",
            "ExpressionAttributeValues" => ddb_item(Dict(":run_id" => run_id)))
        start_key === nothing || (params["ExclusiveStartKey"] = start_key)
        resp = aws_retry() do
            Dynamodb.query(ctx.cfg.jobs_table, params; aws_config=ctx.aws)
        end
        append!(jobs, ddb_parse.(resp["Items"]))
        start_key = get(resp, "LastEvaluatedKey", nothing)
        start_key === nothing && break
    end
    return jobs
end

"Fetch a job log from S3, or `nothing` if the job didn't upload one."
function job_log(ctx::FarmCtx, job::AbstractDict)
    key = get(job, "log_key", nothing)
    key === nothing && return nothing
    resp = S3.get_object(ctx.cfg.bucket, key, Dict("return_raw" => true); aws_config=ctx.aws)
    return String(copy(resp))
end
