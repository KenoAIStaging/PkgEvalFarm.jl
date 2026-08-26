# Integration tests against a local moto server (pip install 'moto[server]'),
# exercising the full run/job lifecycle plus the broker's STS call.

module MotoTests

using Test
using AWS
using Dates
using JSON
using PkgEvalFarm
using Sockets
using UUIDs: UUID, uuid5
import PkgEval

using ..BrokerTests: FarmBroker, with_env

const PEF = PkgEvalFarm

using AWS: @service
@service Auto_Scaling
@service Dynamodb
@service SQS
@service S3
@service SSM

using ..MotoHelpers: MotoHelpers, MotoConfig, start_moto

moto_available = MotoHelpers.moto_available()
if !moto_available
    @warn "moto_server not found; skipping integration tests"
    @test_skip "moto integration"
else

proc, port = start_moto()
endpoint = "http://127.0.0.1:$port"
aws = MotoConfig(endpoint, "us-east-1", AWS.AWSCredentials("testing", "testing"))

try
    # provision the same resources terraform would
    for (table, keys) in ["pkgeval-runs" => [("run_id", "HASH")],
                          "pkgeval-jobs" => [("run_id", "HASH"), ("job_key", "RANGE")]]
        Dynamodb.create_table(
            [Dict("AttributeName" => k, "AttributeType" => "S") for (k, _) in keys],
            [Dict("AttributeName" => k, "KeyType" => t) for (k, t) in keys],
            table, Dict("BillingMode" => "PAY_PER_REQUEST"); aws_config=aws)
    end
    queue_url = SQS.create_queue("pkgeval-jobs"; aws_config=aws)["QueueUrl"]
    slow_queue_url = SQS.create_queue("pkgeval-jobs-slow"; aws_config=aws)["QueueUrl"]
    S3.create_bucket("pkgeval-results"; aws_config=aws)

    cfg = FarmConfig(; region="us-east-1", queue_url, slow_queue_url,
                     runs_table="pkgeval-runs",
                     jobs_table="pkgeval-jobs", bucket="pkgeval-results")
    ctx = FarmCtx(cfg, aws)

    configs = [PkgEval.Configuration(; name="primary", julia="JuliaLang/julia#abc123"),
               PkgEval.Configuration(; name="against", julia="v1.12.0")]
    packages = ["Example", "Crayons", "JSON"]

    @testset "create_run" begin
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs, packages, Dict{String,Any}());
                                submitter="tester")
        global RUN_ID = run_id
        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "expanding"  # all runs start expanding; a worker fans out
        @test run["total_jobs"] == 0
        @test run["completed_jobs"] == 0
        @test length(run["configs"]) == 2
        @test run["packages"] == packages
        @test isempty(PEF.run_jobs(ctx, run_id))
        @test_throws Exception PEF.create_run(ctx, PEF.RunSpec(configs, packages, Dict{String,Any}());
                                              submitter="tester", run_id)  # ids are unique

        # worker-side fan-out of the stored package list
        claimed = PEF.claim_job(ctx; wait=1)
        @test claimed isa PEF.ClaimedExpand
        @test claimed.run_id == run_id
        @test PEF.expand_run(ctx, run_id, String.(run["packages"])) == 6
        SQS.delete_message(claimed.queue_url, claimed.receipt_handle; aws_config=aws)

        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "active"
        @test run["total_jobs"] == 6
        jobs = PEF.run_jobs(ctx, run_id)
        @test length(jobs) == 6
        @test all(j -> j["status"] == "pending", jobs)
        # the scheduler's duration estimate rides along for the bot's ETA
        @test all(j -> j["est"] == PEF.DEFAULT_DURATION_ESTIMATE, jobs)
    end

    @testset "claim/heartbeat/complete lifecycle" begin
        PEF.SLOT_HOURLY_RATE[] = 0.36   # price the fabricated results: 42 s -> $0.0042
        seen = Set{Tuple{String,String}}()
        for i in 1:6
            claimed = PEF.claim_job(ctx; wait=1)
            @test claimed !== nothing
            push!(seen, (claimed.job.config, claimed.job.package))
            @test claimed.attempts == 1

            job_item = only(filter(j -> j["job_key"] == PEF.job_key(claimed.job),
                                   PEF.run_jobs(ctx, RUN_ID)))
            @test job_item["status"] == "running"
            @test job_item["attempts"] == 1

            PEF.heartbeat(ctx, claimed)  # must not throw

            # fabricate results: JSON fails on primary only; Crayons fails everywhere
            status = (claimed.job.package == "JSON" && claimed.job.config == "primary") ? "fail" :
                     claimed.job.package == "Crayons" ? "crash" : "test"
            # one job carries a measured wall time: cost must price the slot
            # occupancy, not the test-phase duration; the rest fall back to
            # duration (a worker predating wall metering)
            wall = claimed.job.package == "JSON" && claimed.job.config == "primary" ? 84.0 : 0.0
            log = "log of $(claimed.job.package) on $(claimed.job.config)"
            # give failures a signature line, as real test logs would have
            status == "fail" &&
                (log = "ERROR: MethodError: no method matching parse(::Foo)\n" * log)
            status == "crash" && (log = "Unreachable reached at 0x1234\n" * log)
            result = PEF.JobResult(; status,
                reason=status == "fail" ? "test_failures" :
                       status == "crash" ? "segfault" : nothing,
                version="1.2.3", duration=42.0, wall, peak_rss=1_234_567_890, log)
            PEF.record_result(ctx, claimed, result)
        end
        @test length(seen) == 6
        PEF.SLOT_HOURLY_RATE[] = nothing
        jobs = PEF.run_jobs(ctx, RUN_ID)
        for j in jobs
            billed = j["job_key"] == "primary#JSON" ? 84.0 : 42.0   # wall beats duration
            @test isapprox(j["cost"], billed / 3600 * 0.36; rtol=1e-6)
        end
        @test only(filter(j -> j["job_key"] == "primary#JSON", jobs))["wall"] == 84.0
        @test all(j -> j["peak_rss"] == 1_234_567_890, jobs)
        # failing jobs get their first error line extracted; passing jobs
        # don't carry the attribute at all
        @test only(filter(j -> j["job_key"] == "primary#JSON", jobs))["error_line"] ==
              "ERROR: MethodError: no method matching parse(::Foo)"
        @test only(filter(j -> j["job_key"] == "primary#Crayons", jobs))["error_line"] ==
              "Unreachable reached at 0x1234"
        @test !haskey(only(filter(j -> j["job_key"] == "against#JSON", jobs)), "error_line")

        run = PEF.get_run(ctx, RUN_ID)
        @test run["completed_jobs"] == 6
        @test run["status"] == "done"

        # queue is drained
        @test PEF.claim_job(ctx; wait=0) === nothing
    end

    @testset "duplicate delivery is harmless" begin
        # simulate SQS at-least-once: re-enqueue an already-finished job
        PEF.enqueue_jobs(ctx, [PEF.JobRef(RUN_ID, "primary", "Example")])
        @test PEF.claim_job(ctx; wait=1) === nothing  # detected as done, message deleted
        @test PEF.claim_job(ctx; wait=0) === nothing  # and not redelivered
        run = PEF.get_run(ctx, RUN_ID)
        @test run["completed_jobs"] == 6  # counter untouched

        # a duplicate of a job still LIVE on another worker (fresh heartbeat
        # stamp) bounces off instead of re-executing a long evaluation...
        Dynamodb.put_item(PEF.ddb_item(Dict(
                "run_id" => RUN_ID, "job_key" => "primary#LongJob",
                "config" => "primary", "package" => "LongJob",
                "status" => "running", "attempts" => 1,
                "started_at" => PEF.isodate(), "heartbeat_at" => PEF.isodate())),
            "pkgeval-jobs"; aws_config=aws)
        PEF.enqueue_jobs(ctx, [PEF.JobRef(RUN_ID, "primary", "LongJob")])
        @test PEF.claim_job(ctx; wait=1) === nothing   # live: duplicate deleted
        # ...but once the beats stop (worker death), redelivery re-claims
        Dynamodb.update_item(
            PEF.ddb_item(Dict("run_id" => RUN_ID, "job_key" => "primary#LongJob")),
            "pkgeval-jobs",
            Dict("UpdateExpression" => "SET heartbeat_at = :old",
                 "ExpressionAttributeValues" => PEF.ddb_item(Dict(
                     ":old" => PEF.isodate(Dates.now(Dates.UTC) - Dates.Minute(20)))));
            aws_config=aws)
        PEF.enqueue_jobs(ctx, [PEF.JobRef(RUN_ID, "primary", "LongJob")])
        c = PEF.claim_job(ctx; wait=1)
        @test c isa PEF.ClaimedJob && c.job.package == "LongJob"
        # and a released job is immediately re-claimable: release clears the
        # stamp precisely so drain fast-release redelivery keeps working
        PEF.release_job(ctx, c; delay=0)
        c = PEF.claim_job(ctx; wait=1)
        @test c isa PEF.ClaimedJob && c.job.package == "LongJob"
        SQS.delete_message(c.queue_url, c.receipt_handle; aws_config=aws)
        Dynamodb.delete_item(PEF.ddb_item(Dict("run_id" => RUN_ID, "job_key" => "primary#LongJob")),
                             "pkgeval-jobs"; aws_config=aws)
    end

    @testset "logs and report" begin
        jobs = PEF.run_jobs(ctx, RUN_ID)
        job = only(filter(j -> j["job_key"] == "primary#JSON", jobs))
        @test endswith(PEF.job_log(ctx, job), "log of JSON on primary")

        # create-only uploads: conditional re-put of an existing log is rejected
        # and the original content survives (first write wins)
        overwrite_err = try
            S3.put_object(cfg.bucket, PEF.log_key(RUN_ID, "primary", "JSON"),
                Dict("body" => "forged",
                     "headers" => Dict("If-None-Match" => "*")); aws_config=aws)
            nothing
        catch err
            err
        end
        @test PEF.is_precondition_failed(overwrite_err)
        @test endswith(PEF.job_log(ctx, job), "log of JSON on primary")

        # report generation runs through FarmBot (the stdlib-only bot Lambda code)
        lite = PEF.FarmLite.LiteCtx(; region="us-east-1",
            creds=PEF.FarmLite.AwsCreds("testing", "testing", nothing),
            queue_url, runs_table=cfg.runs_table, jobs_table=cfg.jobs_table,
            bucket=cfg.bucket, endpoint)
        report = PEF.FarmBot.generate_report(lite, RUN_ID)
        @test occursin("JSON", report.markdown)
        @test occursin("failed on primary but not on against", report.markdown)
        @test occursin("Packages that failed on both", report.markdown)  # Crayons
        @test occursin("package has test failures", report.markdown)  # stored reason_message
        @test occursin("possible new issues: 1 package", report.summary)
        @test report.new_fails == 1  # JSON regressed; Crayons fails on both
        # 5 jobs x 42 s + one 84 s wall-billed at $0.36/slot-h -> $0.0294
        @test occursin("estimated compute cost: \$0.03", report.markdown)
        @test isapprox(report.cost, (5 * 42.0 + 84.0) / 3600 * 0.36; rtol=1e-6)
        @test PEF.FarmBot.dollars(12.3) == "\$12.30"
        @test PEF.FarmBot.dollars(0.0252) == "\$0.03"

        # uploaded artifacts
        fetch_raw(key) = String(copy(S3.get_object(cfg.bucket, key,
            Dict("return_raw" => true); aws_config=aws)))
        @test fetch_raw(PEF.report_key(RUN_ID, "report.md")) == report.markdown
        db = JSON.parse(fetch_raw(PEF.report_key(RUN_ID, "db.json")))
        @test length(db["jobs"]) == 6

        # the interactive report: the page itself lives on GitHub Pages, only
        # its compact per-run dataset is uploaded; the report link carries the
        # run id for the page to find it
        @test PEF.FarmBot.report_url(lite, RUN_ID) ==
              PEF.FarmBot.report_page() * "?run=" * RUN_ID
        rj = JSON.parse(fetch_raw(PEF.report_key(RUN_ID, "report.json")))
        @test rj["run"]["id"] == RUN_ID
        @test rj["run"]["primary"]["repo"] == "JuliaLang/julia"
        @test rj["run"]["primary"]["sha"] == "abc123"
        @test rj["run"]["against"]["sha"] == "v1.12.0"  # not a repo#sha spec
        @test rj["run"]["against"]["repo"] == ""
        @test rj["run"]["total_jobs"] == 6
        # rows: [name, ver, pstatus, preason, pdur, astatus, areason, adur, aver,
        #        plogdir, alogdir]
        rows = Dict(r[1] => r for r in rj["pkgs"])
        @test length(rows) == 3
        @test rows["JSON"][3] == "f" && rows["JSON"][6] == "t"      # the new failure
        @test rows["JSON"][4] >= 0                                  # has a reason...
        @test rj["reasons"][rows["JSON"][4] + 1][1] == "test_failures"
        @test rows["Crayons"][3] == "c" && rows["Crayons"][6] == "c"  # fails on both
        @test rows["Example"][3] == "t" && rows["Example"][6] == "t"
        @test rows["Example"][2] == "1.2.3"
        @test rows["Example"][9] == 0  # same version on both sides
        @test rows["Example"][5] == 42  # duration in whole seconds
        # log locations come from each job's log_key
        @test rj["logdirs"][rows["JSON"][10] + 1] == "runs/$RUN_ID/logs/primary"
        @test rj["logdirs"][rows["JSON"][11] + 1] == "runs/$RUN_ID/logs/against"
        # the new failure's stored error_line becomes a failure signature;
        # Crayons fails on both builds, so it is not clustered
        @test rj["sigs"] == [Dict("label" => "ERROR: MethodError: no method matching parse(::Foo)",
                                  "n" => 1)]
        @test rj["nfsig"] == Dict("JSON" => 0)

        # an all-skip run (sandboxes failing to launch) must warn, never report
        # a clean pass: skips are excluded from the comparison buckets
        skip_run = PEF.create_run(ctx,
            PEF.RunSpec(configs, ["Alpha", "Beta"], Dict{String,Any}());
            submitter="tester")
        expand_claim = PEF.claim_job(ctx; wait=1)
        @test expand_claim isa PEF.ClaimedExpand
        @test PEF.expand_run(ctx, skip_run, ["Alpha", "Beta"]) == 4
        SQS.delete_message(expand_claim.queue_url, expand_claim.receipt_handle;
                           aws_config=aws)
        for _ in 1:4
            c = PEF.claim_job(ctx; wait=1)
            @test c isa PEF.ClaimedJob
            status = c.job.config == "primary" ? "skip" : "test"
            PEF.record_result(ctx, c, PEF.JobResult(; status,
                reason=status == "skip" ? "uninstallable" : nothing,
                duration=1.0, log="log"))
        end
        skip_report = PEF.FarmBot.generate_report(lite, skip_run)
        @test occursin("infrastructure failure", skip_report.summary)
        @test occursin("2 of 2 primary evaluations were skipped", skip_report.summary)
        @test skip_report.new_fails == 0
    end

    @testset "worker error handling" begin
        # a job whose evaluation throws gets released, then errored out at attempt >= 3
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs[1:1], ["Broken"], Dict{String,Any}());
                                submitter="tester")
        expand_claim = PEF.claim_job(ctx; wait=1)
        @test expand_claim isa PEF.ClaimedExpand
        PEF.expand_run(ctx, run_id, ["Broken"])
        SQS.delete_message(expand_claim.queue_url, expand_claim.receipt_handle; aws_config=aws)
        for attempt in 1:3
            claimed = PEF.claim_job(ctx; wait=1)
            @test claimed !== nothing
            @test claimed.job.package == "Broken"
            if attempt < 3
                PEF.release_job(ctx, claimed; delay=0)
            else
                @test claimed.attempts == 3
                PEF.record_result(ctx, claimed, PEF.JobResult(; status="error",
                    reason="worker_exception", log="boom"))
            end
        end
        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "done"
        job = only(PEF.run_jobs(ctx, run_id))
        @test job["status"] == "error"
        @test job["attempts"] == 3
    end

    @testset "retry after a dead attempt records its own log" begin
        # a worker dying between its log upload and its result write leaves the
        # canonical log key taken by a log that may contradict the retry's
        # outcome (e.g. a time-limit kill whose retry passes) — the retry must
        # store and reference its own log, not adopt the dead attempt's
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs[1:1], ["Zombie"], Dict{String,Any}());
                                submitter="tester")
        expand_claim = PEF.claim_job(ctx; wait=1)
        @test expand_claim isa PEF.ClaimedExpand
        PEF.expand_run(ctx, run_id, ["Zombie"])
        SQS.delete_message(expand_claim.queue_url, expand_claim.receipt_handle; aws_config=aws)

        # attempt 1: uploads its log, then dies before recording a result
        S3.put_object(cfg.bucket, PEF.log_key(run_id, "primary", "Zombie"),
            Dict("body" => "PkgEval terminated: time limit",
                 "headers" => Dict("If-None-Match" => "*")); aws_config=aws)

        # attempt 2 passes and must end up pointing at its own log
        claimed = PEF.claim_job(ctx; wait=1)
        @test claimed isa PEF.ClaimedJob
        PEF.record_result(ctx, claimed, PEF.JobResult(; status="test", duration=1.0,
                                                      log="tests passed"))
        job = only(PEF.run_jobs(ctx, run_id))
        @test job["status"] == "test"
        @test job["log_key"] == PEF.log_key(run_id, "primary", "Zombie", 2)
        @test PEF.job_log(ctx, job) == "tests passed"
        # the dead attempt's log stays untouched at the canonical key
        @test String(copy(S3.get_object(cfg.bucket, PEF.log_key(run_id, "primary", "Zombie"),
            Dict("return_raw" => true); aws_config=aws))) == "PkgEval terminated: time limit"
    end

    @testset "expand jobs" begin
        # empty package list => run starts in `expanding` with a single expand message
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs, String[], Dict{String,Any}());
                                submitter="tester")
        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "expanding"
        @test run["total_jobs"] == 0
        @test isempty(PEF.run_jobs(ctx, run_id))

        claimed = PEF.claim_job(ctx; wait=1)
        @test claimed isa PEF.ClaimedExpand
        @test claimed.run_id == run_id
        PEF.heartbeat(ctx, claimed)  # expansion can be slow (may build Julia)

        # what a worker does after computing the package set
        njobs = PEF.expand_run(ctx, run_id, ["Crayons", "Example", "JSON"])
        @test njobs == 6
        SQS.delete_message(claimed.queue_url, claimed.receipt_handle; aws_config=aws)

        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "active"
        @test run["total_jobs"] == 6
        jobs = PEF.run_jobs(ctx, run_id)
        @test length(jobs) == 6

        # baseline reuse: the first run is `done` with an identical `against`
        # config (immutable spec v1.12.0), so its results transferred — those
        # jobs arrive pre-completed, pointing at the donor's logs, and only the
        # primary side was enqueued
        reused = filter(j -> get(j, "reused_from", nothing) !== nothing, jobs)
        @test length(reused) == 3
        @test all(j -> j["config"] == "against", reused)
        @test all(j -> j["reused_from"] == RUN_ID, reused)
        # the donor's error_line rides along for its crashed job, and stays
        # absent for jobs that passed there
        @test only(filter(j -> j["job_key"] == "against#Crayons", jobs))["error_line"] ==
              "Unreachable reached at 0x1234"
        @test !haskey(only(filter(j -> j["job_key"] == "against#JSON", jobs)), "error_line")
        @test only(filter(j -> j["job_key"] == "against#Example", jobs))["status"] == "test"
        @test startswith(only(filter(j -> j["job_key"] == "against#JSON", jobs))["log_key"],
                         "runs/$RUN_ID/")
        @test run["completed_jobs"] == 3

        # a duplicate expand message after the flip only re-enqueues, never resets
        first_claim = PEF.claim_job(ctx; wait=1)
        @test first_claim isa PEF.ClaimedJob
        njobs = PEF.expand_run(ctx, run_id, ["Crayons", "Example", "JSON"])  # redelivery scenario
        @test njobs == 6
        job_item = only(filter(j -> j["job_key"] == PEF.job_key(first_claim.job),
                               PEF.run_jobs(ctx, run_id)))
        @test job_item["status"] == "running"  # not reset back to pending

        # drain: claim everything (incl. duplicates) and finish the run
        PEF.record_result(ctx, first_claim, PEF.JobResult(; status="test", duration=1.0))
        for _ in 1:20  # claim_job also returns nothing for swallowed duplicates
            PEF.get_run(ctx, run_id)["status"] == "done" && break
            c = PEF.claim_job(ctx; wait=1)
            c isa PEF.ClaimedJob || continue
            PEF.record_result(ctx, c, PEF.JobResult(; status="test", duration=1.0))
        end
        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "done"
        @test run["completed_jobs"] == 6

        # the report must point reused baseline logs at the donor run's prefix,
        # and jobs recorded without a log get no location (-1)
        lite = PEF.FarmLite.LiteCtx(; region="us-east-1",
            creds=PEF.FarmLite.AwsCreds("testing", "testing", nothing),
            queue_url, runs_table=cfg.runs_table, jobs_table=cfg.jobs_table,
            bucket=cfg.bucket, endpoint)
        PEF.FarmBot.generate_report(lite, run_id)
        rj = JSON.parse(String(copy(S3.get_object(cfg.bucket,
            PEF.report_key(run_id, "report.json"),
            Dict("return_raw" => true); aws_config=aws))))
        rrows = Dict(r[1] => r for r in rj["pkgs"])
        @test rj["logdirs"][rrows["JSON"][11] + 1] == "runs/$RUN_ID/logs/against"
        @test rrows["JSON"][10] == -1  # drained via JobResult without a log
    end

    @testset "fresh baseline opts out of reuse" begin
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs, ["Example"], Dict{String,Any}());
                                submitter="tester", reuse=false)
        # the previous testset may leave swallowed-duplicate messages behind, so
        # claim until the expand message surfaces
        claimed = nothing
        for _ in 1:10
            claimed = PEF.claim_job(ctx; wait=1)
            claimed isa PEF.ClaimedExpand && break
        end
        @test claimed isa PEF.ClaimedExpand
        @test PEF.expand_run(ctx, run_id, ["Example"]) == 2
        SQS.delete_message(claimed.queue_url, claimed.receipt_handle; aws_config=aws)

        jobs = PEF.run_jobs(ctx, run_id)
        @test length(jobs) == 2
        @test all(j -> j["status"] == "pending", jobs)   # nothing was reused
        @test all(j -> !haskey(j, "reused_from"), jobs)
        @test PEF.get_run(ctx, run_id)["completed_jobs"] == 0

        # drain, so later testsets start from an empty queue (stale duplicates
        # from earlier testsets may be swallowed along the way)
        for _ in 1:12
            PEF.get_run(ctx, run_id)["status"] == "done" && break
            c = PEF.claim_job(ctx; wait=1)
            c isa PEF.ClaimedJob || continue
            PEF.record_result(ctx, c, PEF.JobResult(; status="test", duration=1.0))
        end
        @test PEF.get_run(ctx, run_id)["status"] == "done"
    end

    @testset "slow queue has priority" begin
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs, ["Zebra"], Dict{String,Any}());
                                submitter="tester", reuse=false)
        expand = nothing
        for _ in 1:10
            expand = PEF.claim_job(ctx; wait=1)
            expand isa PEF.ClaimedExpand && break
        end
        @test expand isa PEF.ClaimedExpand
        @test expand.queue_url == slow_queue_url   # expand messages ride the slow queue
        @test PEF.expand_run(ctx, run_id, ["Zebra"]) == 2
        SQS.delete_message(expand.queue_url, expand.receipt_handle; aws_config=aws)

        # duplicate one job onto the slow queue: despite being sent *after* the
        # fast-queue messages, it must be claimed first
        PEF.enqueue_jobs(ctx, [PEF.JobRef(run_id, "against", "Zebra")];
                         queue_url=PEF.slow_queue(cfg))
        claimed = PEF.claim_job(ctx; wait=1)
        @test claimed isa PEF.ClaimedJob
        @test claimed.queue_url == PEF.slow_queue(cfg)
        @test claimed.job.config == "against" && claimed.job.package == "Zebra"
        PEF.heartbeat(ctx, claimed)   # visibility ops target the right queue
        PEF.record_result(ctx, claimed, PEF.JobResult(; status="test", duration=1.0))

        # drain the fast queue: one real job plus one now-stale duplicate
        for _ in 1:12
            PEF.get_run(ctx, run_id)["status"] == "done" && break
            c = PEF.claim_job(ctx; wait=1)
            c isa PEF.ClaimedJob || continue
            PEF.record_result(ctx, c, PEF.JobResult(; status="test", duration=1.0))
        end
        @test PEF.get_run(ctx, run_id)["status"] == "done"
    end

    @testset "duration estimates prefer covering donors" begin
        # a streak of small recent runs must not crowd an ecosystem-scale donor
        # out of the max_donors window: that leaves the whole request on the
        # default estimate and un-front-loads the exact jobs the slow queue
        # exists for (seen live: LinearAlgebra unestimated, 11-hour run tail).
        # Job items only — a fake `done` run item would sidetrack the bot's
        # report poll in later testsets, so the donor tuples are hand-built.
        put_job(run, pkg, dur) = Dynamodb.put_item(PEF.ddb_item(Dict(
            "run_id" => run, "job_key" => "primary#$pkg", "config" => "primary",
            "package" => pkg, "status" => "test", "duration" => dur)),
            cfg.jobs_table; aws_config=aws)
        put_job("big-donor", "Alpha", 100.0)
        put_job("big-donor", "Beta", 8000.0)
        put_job("big-donor", "Gamma", 50.0)
        for i in 1:3   # newer, but covering only Alpha
            put_job("small-donor-$i", "Alpha", 10.0)
        end
        donors = [("2026-02-03T00:00:00Z", "small-donor-3", Any[], 1),
                  ("2026-02-02T00:00:00Z", "small-donor-2", Any[], 1),
                  ("2026-02-01T00:00:00Z", "small-donor-1", Any[], 1),
                  ("2026-01-01T00:00:00Z", "big-donor", Any[], 3)]
        est = PEF.duration_estimates(ctx, ["Alpha", "Beta", "Gamma"], donors)
        @test est["Beta"] == 8000.0    # would be absent under newest-3 alone
        @test est["Gamma"] == 50.0
        @test est["Alpha"] == 100.0    # max over donors, not first-donor-wins
        # and completed_runs supplies the total_jobs the preference keys on
        @test any(d -> d[4] >= 6, PEF.completed_runs(ctx))
    end

    @testset "submitter requirement parsing" begin
        # "TEAM" gates on a GITHUB_ORG team; "ORG/TEAM" carries its own org
        # (matching the broker's spec format); "" is plain org membership
        withenv("GITHUB_ORG" => "JuliaLang", "SUBMITTER_TEAM" => "pkgeval-submitters") do
            @test PEF.FarmBot.submitter_requirement() == ("JuliaLang", "pkgeval-submitters")
        end
        withenv("GITHUB_ORG" => "JuliaLang", "SUBMITTER_TEAM" => "JuliaCI/pkgeval-workers") do
            @test PEF.FarmBot.submitter_requirement() == ("JuliaCI", "pkgeval-workers")
        end
        withenv("GITHUB_ORG" => "JuliaLang", "SUBMITTER_TEAM" => "") do
            @test PEF.FarmBot.submitter_requirement() == ("JuliaLang", "")
        end
    end

    @testset "bot end-to-end (stub GitHub)" begin
        import HTTP as TestHTTP

        posted = String[]  # comment bodies the bot posts
        gh_base = Ref("")
        notifications = Ref("[]")

        router = TestHTTP.Router()
        TestHTTP.register!(router, "GET", "/notifications",
            req -> TestHTTP.Response(200, notifications[]))
        TestHTTP.register!(router, "PATCH", "/notifications/threads/*",
            req -> TestHTTP.Response(205))
        TestHTTP.register!(router, "GET", "/repos/JuliaLang/julia/issues/comments/1",
            req -> TestHTTP.Response(200, JSON.json(Dict(
                "id" => 987654,
                "body" => "@pkgeval runtests([\"Example\"])",
                "user" => Dict("login" => "keno")))))
        # same command comment as the webhook test delivers (id 555111), for the
        # webhook+poll overlap check
        TestHTTP.register!(router, "GET", "/repos/JuliaLang/julia/issues/comments/2",
            req -> TestHTTP.Response(200, JSON.json(Dict(
                "id" => 555111,
                "body" => "@pkgeval runtests([\"Example\"])",
                "user" => Dict("login" => "keno")))))
        TestHTTP.register!(router, "GET", "/repos/JuliaLang/julia/issues/12345",
            req -> TestHTTP.Response(200, JSON.json(Dict(
                "number" => 12345,
                "repository_url" => "$(gh_base[])/repos/JuliaLang/julia",
                "pull_request" => Dict("url" => "$(gh_base[])/repos/JuliaLang/julia/pulls/12345")))))
        TestHTTP.register!(router, "GET", "/repos/JuliaLang/julia/pulls/12345",
            req -> TestHTTP.Response(200, JSON.json(Dict(
                "head" => Dict("sha" => "abcdef123456"),
                "base" => Dict("ref" => "master")))))
        # author authorization: keno is an active submitter, rando is nobody
        TestHTTP.register!(router, "GET",
            "/orgs/KenoAIStaging/teams/pkgeval-submitters/memberships/keno",
            req -> TestHTTP.Response(200, JSON.json(Dict("state" => "active"))))
        TestHTTP.register!(router, "GET",
            "/orgs/KenoAIStaging/teams/pkgeval-submitters/memberships/rando",
            req -> TestHTTP.Response(404, "{}"))
        TestHTTP.register!(router, "GET", "/orgs/KenoAIStaging/members/keno",
            req -> TestHTTP.Response(204))
        TestHTTP.register!(router, "GET", "/orgs/KenoAIStaging/members/rando",
            req -> TestHTTP.Response(404, "{}"))
        ENV["GITHUB_ORG"] = "KenoAIStaging"
        ENV["SUBMITTER_TEAM"] = "pkgeval-submitters"
        TestHTTP.register!(router, "POST", "/repos/JuliaLang/julia/issues/12345/comments",
            req -> begin
                push!(posted, JSON.parse(String(req.body))["body"])
                TestHTTP.Response(201, JSON.json(Dict("id" => 700000 + length(posted))))
            end)
        edited = Pair{String,String}[]  # comment id => body, from PATCH edits
        TestHTTP.register!(router, "PATCH", "/repos/JuliaLang/julia/issues/comments/*",
            req -> begin
                push!(edited, String(split(req.target, '/')[end]) =>
                              JSON.parse(String(req.body))["body"])
                TestHTTP.Response(200, "{}")
            end)
        deleted = String[]  # comment ids removed (the status comment, on delivery)
        TestHTTP.register!(router, "DELETE", "/repos/JuliaLang/julia/issues/comments/*",
            req -> begin
                push!(deleted, String(split(req.target, '/')[end]))
                TestHTTP.Response(204)
            end)
        gh_port, server = let p = 0, srv = nothing
            for attempt in 1:10   # random ports collide occasionally; retry
                p = rand(30001:40000)
                try
                    srv = TestHTTP.serve!(router, "127.0.0.1", p)
                    break
                catch err
                    attempt == 10 && rethrow()
                end
            end
            p, srv
        end
        gh_base[] = "http://127.0.0.1:$gh_port"
        SQS.purge_queue(queue_url; aws_config=aws)  # drop strays from earlier testsets
        gh = PEF.FarmLite.GitHubCtx("bot-token", gh_base[])

        lite = PEF.FarmLite.LiteCtx(; region="us-east-1",
            creds=PEF.FarmLite.AwsCreds("testing", "testing", nothing),
            queue_url, runs_table=cfg.runs_table, jobs_table=cfg.jobs_table,
            bucket=cfg.bucket, endpoint)

        # version-tag pinning: "1.9.9" only resolves under its tag name "v1.9.9"
        pin_sha = "a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d0"
        TestHTTP.register!(router, "GET", "/repos/JuliaLang/julia/commits/v1.9.9",
            req -> TestHTTP.Response(200, JSON.json(Dict("sha" => pin_sha))))

        try
            @test PEF.FarmBot.resolve_vs(":master", "JuliaLang/julia") == "JuliaLang/julia#master"
            @test PEF.FarmBot.resolve_vs("#1.12.6", "JuliaLang/julia") == "JuliaLang/julia#1.12.6"
            @test PEF.FarmBot.resolve_vs("1.12.6", "JuliaLang/julia") == "JuliaLang/julia#1.12.6"
            @test PEF.FarmBot.resolve_vs("v1.12.6", "JuliaLang/julia") == "JuliaLang/julia#v1.12.6"
            @test PEF.FarmBot.resolve_vs("other/repo#branch", "JuliaLang/julia") == "other/repo#branch"
            @test PEF.FarmBot.pin_commit(gh, "JuliaLang/julia#1.9.9") == "JuliaLang/julia#$pin_sha"
            @test PEF.FarmBot.pin_commit(gh, "JuliaLang/julia#nosuchref") == "JuliaLang/julia#nosuchref"

            # 1. a mention arrives -> bot submits a run and acks
            notifications[] = JSON.json([Dict(
                "id" => "42", "reason" => "mention",
                "subject" => Dict("type" => "PullRequest",
                    "url" => "$(gh_base[])/repos/JuliaLang/julia/issues/12345",
                    "latest_comment_url" => "$(gh_base[])/repos/JuliaLang/julia/issues/comments/1"))])
            PEF.FarmBot.handle_invocation(lite, gh)
            @test length(posted) == 1
            @test occursin("has been submitted as run", posted[1])
            @test occursin("JuliaLang/julia#abcdef123456", posted[1])
            run_id = match(r"run `([^`]+)`", posted[1]).captures[1]

            run = PEF.get_run(ctx, run_id)
            @test run["status"] == "expanding"
            @test run["packages"] == ["Example"]
            @test run["context"]["repo"] == "JuliaLang/julia"
            @test run["submitter"] == "keno via @pkgeval"
            # the triggering comment is recorded (its id is also the
            # comment-derived run id's suffix) so the report links straight
            # to the comment that started the run
            @test run["context"]["comment"] == parse(Int, chopprefix(run_id, "gh-"))
            @test run["configs"][1]["name"] == "primary"
            @test run["configs"][1]["julia"] == "JuliaLang/julia#abcdef123456"
            @test run["configs"][2]["julia"] == "JuliaLang/julia#master"
            # the config json round-trips into a PkgEval Configuration
            config = PEF.config_from_dict(run["configs"][1])
            @test config.buildflags == ["LLVM_ASSERTIONS=1", "FORCE_ASSERTIONS=1"]
            # the submission comment id is recorded, and the same invocation
            # already edits an in-place status body over the ack text
            @test run["comment_id"] == 700001
            @test !isempty(edited) && edited[end].first == "700001"
            @test occursin("**Status**", edited[end].second)
            @test occursin("expanding", edited[end].second)

            # 2. a worker picks it up, expands and completes it
            notifications[] = "[]"
            expand_claim = PEF.claim_job(ctx; wait=1)
            @test expand_claim isa PEF.ClaimedExpand
            PEF.expand_run(ctx, run_id, ["Example"])
            SQS.delete_message(expand_claim.queue_url, expand_claim.receipt_handle; aws_config=aws)
            recorded = 0
            for _ in 1:20  # claim_job also returns nothing for swallowed duplicates
                PEF.get_run(ctx, run_id)["status"] == "done" && break
                c = PEF.claim_job(ctx; wait=1)
                c isa PEF.ClaimedJob || continue
                PEF.record_result(ctx, c, PEF.JobResult(; status="test", version="1.0.0",
                                                        duration=1.0, log="ok"))
                if (recorded += 1) == 1
                    # 2b. mid-run, the hourly tick publishes a work-in-progress
                    #     report (min_interval=0 sidesteps the submission
                    #     edit's throttle claim)
                    PEF.FarmBot.update_status_comment(lite, gh,
                        PEF.FarmBot.get_run(lite, String(run_id));
                        min_interval=Dates.Minute(0))
                    wip = JSON.parse(String(copy(S3.get_object(cfg.bucket,
                        PEF.report_key(run_id, "report.json"), Dict("return_raw" => true);
                        aws_config=aws))))
                    @test wip["run"]["in_progress"] == true
                    @test wip["run"]["done_jobs"] == 1
                    @test haskey(wip["run"], "as_of")
                    @test length(wip["pkgs"]) <= 1   # only terminal jobs appear
                end
            end
            @test PEF.get_run(ctx, run_id)["status"] == "done"
            # the counter bumps stamped the dashboard's activity clock
            @test haskey(PEF.get_run(ctx, run_id), "updated_at")

            # 3. next poll posts the report as a *new* comment (edits send no
            #    email notification) and removes the superseded status comment
            PEF.FarmBot.handle_invocation(lite, gh)
            @test length(posted) == 2
            @test occursin("@keno: run `$run_id` finished", posted[end])
            @test occursin("no new package failures", posted[end])
            @test deleted == ["700001"]   # the status comment is gone
            # the published report anchors its trigger link on the comment
            rj = JSON.parse(String(copy(S3.get_object(cfg.bucket,
                PEF.report_key(run_id, "report.json"), Dict("return_raw" => true);
                aws_config=aws))))
            @test rj["run"]["trigger_label"] == "JuliaLang/julia#12345"
            @test rj["run"]["trigger_url"] ==
                  "https://github.com/JuliaLang/julia/pull/12345#issuecomment-" *
                  chopprefix(run_id, "gh-")
            # the final report replaced the work-in-progress snapshot outright
            @test !haskey(rj["run"], "in_progress")
            @test occursin("?run=$run_id", posted[end])

            # 4. and does not double-report
            n_edited = length(edited)
            PEF.FarmBot.handle_invocation(lite, gh)
            @test length(posted) == 2 && length(edited) == n_edited

            # 5. webhook path: an issue_comment delivery submits a run with a
            #    single GitHub call (no notifications involved)
            secret = "hooksecret"
            payload = JSON.json(Dict(
                "action" => "created",
                "comment" => Dict("id" => 555111,
                                  "body" => "@pkgeval runtests([\"Example\"])",
                                  "user" => Dict("login" => "keno")),
                "issue" => Dict("number" => 12345,
                                "pull_request" => Dict("url" => "$(gh_base[])/repos/JuliaLang/julia/pulls/12345")),
                "repository" => Dict("full_name" => "JuliaLang/julia")))
            sign(body) = "sha256=" * bytes2hex(PEF.FarmLite.hmac(Vector{UInt8}(secret), body))
            webhook_event(body, sig) = JSON.json(Dict(
                "requestContext" => Dict("http" => Dict("method" => "POST")),
                "rawPath" => "/",
                "headers" => Dict("x-hub-signature-256" => sig,
                                  "x-github-event" => "issue_comment"),
                "body" => body, "isBase64Encoded" => false))
            with_env(Dict("GITHUB_WEBHOOK_SECRET" => secret)) do
                # bad signature is rejected without side effects
                resp = JSON.parse(PEF.FarmBot.handle_event(webhook_event(payload, sign("evil")), lite, gh))
                @test resp["statusCode"] == 401
                @test length(posted) == 2

                resp = JSON.parse(PEF.FarmBot.handle_event(webhook_event(payload, sign(payload)), lite, gh))
                @test resp["statusCode"] == 200
            end
            @test length(posted) == 3
            @test occursin("has been submitted as run", posted[3])
            webhook_run_id = match(r"run `([^`]+)`", posted[3]).captures[1]
            @test webhook_run_id == "gh-555111"  # deterministic, comment-derived

            # 5a. duplicate deliveries collapse into the one run: a webhook
            # redelivery and the fallback poll rediscovering the same comment
            # both submit nothing and post nothing
            with_env(Dict("GITHUB_WEBHOOK_SECRET" => secret)) do
                resp = JSON.parse(PEF.FarmBot.handle_event(
                    webhook_event(payload, sign(payload)), lite, gh))
                @test resp["statusCode"] == 200
            end
            notifications[] = JSON.json([Dict(
                "id" => "43", "reason" => "mention",
                "subject" => Dict("type" => "PullRequest",
                    "url" => "$(gh_base[])/repos/JuliaLang/julia/issues/12345",
                    "latest_comment_url" => "$(gh_base[])/repos/JuliaLang/julia/issues/comments/2"))])
            PEF.FarmBot.handle_invocation(lite, gh)
            notifications[] = "[]"
            @test length(posted) == 3      # no extra ack comments
            # ... but that poll did edit a status body onto the webhook run's comment
            @test edited[end].first == "700003"
            @test occursin("`gh-555111`", edited[end].second)
            run = PEF.get_run(ctx, "gh-555111")
            @test run["status"] == "expanding"  # still exactly one run, untouched

            # 5b. an unauthorized author gets a refusal and no run
            intruder = replace(payload, "\"login\":\"keno\"" => "\"login\":\"rando\"")
            with_env(Dict("GITHUB_WEBHOOK_SECRET" => secret)) do
                resp = JSON.parse(PEF.FarmBot.handle_event(
                    webhook_event(intruder, sign(intruder)), lite, gh))
                @test resp["statusCode"] == 200
            end
            @test length(posted) == 4
            @test occursin("only members of the KenoAIStaging/pkgeval-submitters team", posted[4])
            posted_refusal = pop!(posted)  # keep later indices stable

            # both authorization modes, checked directly
            @test PEF.FarmBot.authorized_submitter(gh, "keno")
            @test !PEF.FarmBot.authorized_submitter(gh, "rando")
            with_env(Dict("SUBMITTER_TEAM" => "")) do  # org-membership mode
                @test PEF.FarmBot.authorized_submitter(gh, "keno")
                @test !PEF.FarmBot.authorized_submitter(gh, "rando")
            end
            run = PEF.get_run(ctx, webhook_run_id)
            @test run["packages"] == ["Example"]
            @test run["submitter"] == "keno via @pkgeval"

            # 6. stream path: a run flipping to done triggers the report directly
            expand_claim = PEF.claim_job(ctx; wait=1)
            @test expand_claim isa PEF.ClaimedExpand
            PEF.expand_run(ctx, webhook_run_id, ["Example"])
            SQS.delete_message(expand_claim.queue_url, expand_claim.receipt_handle; aws_config=aws)
            for _ in 1:20
                PEF.get_run(ctx, webhook_run_id)["status"] == "done" && break
                c = PEF.claim_job(ctx; wait=1)
                c isa PEF.ClaimedJob || continue
                PEF.record_result(ctx, c, PEF.JobResult(; status="test", duration=1.0))
            end
            @test PEF.get_run(ctx, webhook_run_id)["status"] == "done"

            run_item = PEF.FarmBot.get_run(lite, String(webhook_run_id))
            stream_event = JSON.json(Dict("Records" => [Dict(
                "eventName" => "MODIFY",
                "dynamodb" => Dict("NewImage" => JSON.parse(PEF.FarmLite.json_item(run_item))))]))
            @test JSON.parse(PEF.FarmBot.handle_event(stream_event, lite, gh))["ok"] == true
            @test length(posted) == 4  # a fresh report comment...
            @test occursin("@keno: run `$webhook_run_id` finished", posted[end])
            @test deleted[end] == "700003"   # ...replacing the status comment
            # reporting also annotated the run item with the scalars the
            # dashboard's verdict chips read straight off its table scan
            annotated = PEF.get_run(ctx, webhook_run_id)
            @test annotated["new_fails"] == 0
            @test annotated["cost"] == 0  # no job carried cost metering here

            # duplicate stream delivery does not double-report
            n_edited6 = length(edited)
            @test JSON.parse(PEF.FarmBot.handle_event(stream_event, lite, gh))["ok"] == true
            @test length(posted) == 4 && length(edited) == n_edited6

            # 7. an unrecognized event falls back to the scheduled poll
            notifications[] = "[]"
            @test JSON.parse(PEF.FarmBot.handle_event("{}", lite, gh))["ok"] == true
            @test length(posted) == 4 && length(edited) == n_edited6  # all runs done

            # 8. a worker-failed run (e.g. its Julia build failed) is reported
            #    with the recorded reason on the next poll
            failed_id = PEF.create_run(ctx,
                PEF.RunSpec(configs[1:1], ["Example"], Dict{String,Any}(
                    "repo" => "JuliaLang/julia", "issue" => 12345, "requester" => "keno"));
                submitter="keno via @pkgeval")
            PEF.fail_run(ctx, failed_id,
                "the Julia build for JuliaLang/julia@deadbeefde (linuxassert) failed: http://bk/7")
            PEF.FarmBot.handle_invocation(lite, gh)
            @test occursin("@keno: run `$failed_id` **failed** — the Julia build", posted[end])
            @test occursin("http://bk/7", posted[end])
            n8p, n8e = length(posted), length(edited)
            PEF.FarmBot.handle_invocation(lite, gh)  # reported exactly once
            @test length(posted) == n8p && length(edited) == n8e

            # ...and via the stream path: a run flipping to failed reports
            # immediately, without waiting for the next poll
            failed2 = PEF.create_run(ctx,
                PEF.RunSpec(configs[1:1], ["Example"], Dict{String,Any}(
                    "repo" => "JuliaLang/julia", "issue" => 12345, "requester" => "keno"));
                submitter="keno via @pkgeval")
            PEF.fail_run(ctx, failed2, "its Julia build failed: http://bk/8")
            run_item2 = PEF.FarmBot.get_run(lite, String(failed2))
            stream2 = JSON.json(Dict("Records" => [Dict(
                "eventName" => "MODIFY",
                "dynamodb" => Dict("NewImage" => JSON.parse(PEF.FarmLite.json_item(run_item2))))]))
            @test JSON.parse(PEF.FarmBot.handle_event(stream2, lite, gh))["ok"] == true
            @test occursin("@keno: run `$failed2` **failed** — its Julia build failed", posted[end])
            n8p2 = length(posted)
            @test JSON.parse(PEF.FarmBot.handle_event(stream2, lite, gh))["ok"] == true  # no double
            @test length(posted) == n8p2

            # retire the failed runs' stray expand messages
            while (c = PEF.claim_job(ctx; wait=1)) !== nothing
                SQS.delete_message(c.queue_url, c.receipt_handle; aws_config=aws)
            end
        finally
            close(server)
        end
    end

    @testset "status comment bodies and ETA" begin
        FB = PEF.FarmBot
        now = DateTime(2026, 7, 28, 12, 0, 0)
        # no baseline, no progress, nothing left, or garbage timestamp -> no ETA
        @test FB.eta_from_work("", -1.0, 10.0, 100.0, now) === nothing
        @test FB.eta_from_work("2026-07-28T11:00:00Z", 10.0, 10.0, 100.0, now) === nothing
        @test FB.eta_from_work("2026-07-28T11:00:00Z", 5.0, 100.0, 0.0, now) === nothing
        @test FB.eta_from_work("garbage", 5.0, 10.0, 100.0, now) === nothing
        # 3600 work-seconds done in the last hour (one busy slot), 7200
        # estimated remaining -> two hours out
        eta = FB.eta_from_work("2026-07-28T11:00:00Z", 0.0, 3600.0, 7200.0, now)
        @test eta == DateTime(2026, 7, 28, 14, 0, 0)

        # run_work: actual durations for the finished, estimates for the rest,
        # mean-of-finished fallback for jobs without a stored estimate
        mk(st; dur=nothing, est=nothing) = PEF.FarmLite.Item(
            "status" => PEF.FarmLite.attr(st),
            (dur === nothing ? () : ("duration" => PEF.FarmLite.attr(dur),))...,
            (est === nothing ? () : ("est" => PEF.FarmLite.attr(est),))...)
        jobs = [mk("test"; dur=100.0, est=50.0), mk("fail"; dur=300.0),
                mk("pending"; est=500.0), mk("running")]
        @test FB.run_work(jobs) == (400.0, 700.0)   # 500 est + 200 fallback
        @test FB.run_work([mk("pending"; est=500.0), mk("pending")]) == (0.0, -1.0)
        @test FB.run_work([mk("test"; dur=60.0), mk("pending"; est=30.0)]) == (60.0, 30.0)

        body = FB.status_comment_body("run-1", "primary: `a`, against: `b`",
                                      "active", 80, 200, now, eta)
        @test occursin("run `run-1` (primary: `a`, against: `b`)", body)
        @test occursin("80/200 jobs completed", body)
        @test occursin("Estimated completion: 2026-07-28 14:00 UTC (~2h 0m left)", body)
        @test FB.remaining_str(now, now + Dates.Minute(45)) == "45m"
        @test FB.remaining_str(now, now + Dates.Minute(200)) == "3h 20m"
        @test FB.remaining_str(now, now + Dates.Hour(52)) == "2d 4h"
        @test FB.remaining_str(now, now - Dates.Hour(1)) == "0m"  # never negative
        body = FB.status_comment_body("run-1", "", "expanding", 0, 0, now, nothing)
        @test occursin("expanding — building Julia", body)
        @test occursin("as of 2026-07-28 12:00 UTC", body)
        @test !occursin("Estimated", body)
        # active but no ETA yet: progress without a prediction
        body = FB.status_comment_body("run-1", "", "active", 5, 200, now, nothing)
        @test occursin("5/200 jobs completed.", body)
        @test !occursin("Estimated", body)

        @test FB.configs_summary("[{\"name\":\"primary\",\"julia\":\"x#1\"},{\"name\":\"against\",\"julia\":\"#1.12\"}]") ==
              "primary: `x#1`, against: `#1.12`"
        @test FB.configs_summary("garbage") == ""

        # the hand-rolled isodate parser (trim-safe replacement for DateTime(str, df))
        @test FB.parse_isodate("2026-07-28T15:30:00Z") == DateTime(2026, 7, 28, 15, 30, 0)
        @test FB.parse_isodate(FB.isodate(DateTime(2024, 2, 29, 23, 59, 59))) ==
              DateTime(2024, 2, 29, 23, 59, 59)
        @test FB.parse_isodate("2026-02-29T00:00:00Z") === nothing  # not a leap year
        @test FB.parse_isodate("2026-07-28 15:30:00Z") === nothing
        @test FB.parse_isodate("") === nothing

        # the int accessor's default variant (added for comment_id)
        item = PEF.FarmLite.Item("n" => PEF.FarmLite.attr(7))
        @test PEF.FarmLite.int(item, "n", 0) == 7
        @test PEF.FarmLite.int(item, "missing", 42) == 42
    end

    @testset "build request on missing staged Julia" begin
        # no broker configured => explicit failure, no exception (the positive
        # path is a Lambda.invoke, exercised live; moto cannot run our binary)
        miss = PkgEval.MissingStagedBuild("JuliaLang/julia",
            "1234567890abcdef1234567890abcdef12345678", "linuxassert")
        @test PEF.request_julia_build(ctx, miss) == (:error, nothing)
    end

    @testset "slot pricing" begin
        @test PEF.SLOT_HOURLY_RATE[] === nothing   # non-EC2: unpriced
        withenv("PKGEVAL_SLOT_HOURLY" => "0.5") do
            PEF.init_slot_rate!(ctx, 32)
        end
        @test PEF.SLOT_HOURLY_RATE[] == 0.5
        PEF.SLOT_HOURLY_RATE[] = nothing
        PEF.init_slot_rate!(ctx, 32)   # no env, no instance identity: stays unpriced
        @test PEF.SLOT_HOURLY_RATE[] === nothing

        item = PEF.FarmLite.Item("x" => PEF.FarmLite.attr(1.25))
        @test PEF.FarmLite.flt(item, "x", 0.0) == 1.25
        @test PEF.FarmLite.flt(item, "missing", 7.0) == 7.0
    end

    @testset "worker fail_run" begin
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs[1:1], ["Example"], Dict{String,Any}());
                                submitter="tester")
        PEF.fail_run(ctx, run_id, "the Julia build failed: http://bk/9")
        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "failed"
        @test run["failure_reason"] == "the Julia build failed: http://bk/9"
        # terminal: a later failure path racing this one loses quietly
        PEF.fail_run(ctx, run_id, "another reason")
        @test PEF.get_run(ctx, run_id)["failure_reason"] == "the Julia build failed: http://bk/9"
        # retire the run's stray expand message so later testsets don't claim it
        while (c = PEF.claim_job(ctx; wait=1)) !== nothing
            SQS.delete_message(c.queue_url, c.receipt_handle; aws_config=aws)
        end
    end

    @testset "run waiting marker" begin
        # workers stamp which builds a stalled run waits on; progress clears it
        sha1 = "1234567890abcdef1234567890abcdef12345678"
        sha2 = "abcdef1234567890abcdef1234567890abcdef12"
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs[1:1], ["Example"], Dict{String,Any}());
                                submitter="tester")
        # while expanding: the first note lands, without a URL yet
        PEF.note_run_waiting(ctx, run_id; config="primary", sha=sha1, variant="linux")
        run = PEF.get_run(ctx, run_id)
        @test run["waiting"]["primary"]["sha"] == sha1
        @test run["waiting"]["primary"]["url"] == ""
        # a retry that learned the Buildkite URL fills it in
        PEF.note_run_waiting(ctx, run_id; config="primary", sha=sha1, variant="linux",
                             url="http://bk/42")
        @test PEF.get_run(ctx, run_id)["waiting"]["primary"]["url"] == "http://bk/42"
        # the baseline's own missing build gets its own slot: both are shown
        PEF.note_run_waiting(ctx, run_id; config="against", sha=sha2, variant="linuxassert",
                             url="http://bk/43")
        run = PEF.get_run(ctx, run_id)
        @test run["waiting"]["primary"]["url"] == "http://bk/42"
        @test run["waiting"]["against"]["sha"] == sha2
        @test run["waiting"]["against"]["variant"] == "linuxassert"

        # expansion completing clears the marker
        claimed = PEF.claim_job(ctx; wait=1)
        @test claimed isa PEF.ClaimedExpand
        @test PEF.expand_run(ctx, run_id, ["Example"]) == 1
        SQS.delete_message(claimed.queue_url, claimed.receipt_handle; aws_config=aws)
        @test !haskey(PEF.get_run(ctx, run_id), "waiting")

        # ...as does any job recording a result
        PEF.note_run_waiting(ctx, run_id; config="primary", sha=sha1, variant="linux",
                             url="http://bk/42")
        @test haskey(PEF.get_run(ctx, run_id), "waiting")
        c = PEF.claim_job(ctx; wait=1)
        @test c isa PEF.ClaimedJob
        PEF.record_result(ctx, c, PEF.JobResult(; status="test", duration=1.0))
        run = PEF.get_run(ctx, run_id)
        @test !haskey(run, "waiting")
        @test run["status"] == "done"
    end

    @testset "build-request claim/release" begin
        # BuildRequest's dedup: claimed once, poisoned claims releasable
        Dynamodb.create_table([Dict("AttributeName" => "build_key", "AttributeType" => "S")],
                              [Dict("AttributeName" => "build_key", "KeyType" => "HASH")],
                              "pkgeval-builds", Dict("BillingMode" => "PAY_PER_REQUEST");
                              aws_config=aws)
        m = Module()
        Base.include(m, joinpath(@__DIR__, "..", "buildreq", "src", "BuildRequest.jl"))
        BuildRequest = getfield(m, :BuildRequest)
        FL = BuildRequest.FarmLite
        lctx = FL.LiteCtx(; region="us-east-1",
                          creds=FL.AwsCreds("testing", "testing", nothing),
                          queue_url="unused", runs_table="unused", jobs_table="unused",
                          bucket="unused", endpoint="http://127.0.0.1:$port")
        key = "abc123/linux"
        @test BuildRequest.claim_build(lctx, "pkgeval-builds", key, "test")
        @test !BuildRequest.claim_build(lctx, "pkgeval-builds", key, "test")  # deduped
        BuildRequest.release_build_claim(lctx, "pkgeval-builds", key)
        @test BuildRequest.claim_build(lctx, "pkgeval-builds", key, "test")  # retryable

        # direct-invoke payloads (no requestContext) are accepted by the handler
        resp = BuildRequest.handle_event(
            "{\"repo\":\"JuliaLang/julia\",\"sha\":\"short\",\"variant\":\"linux\"}", lctx)
        @test occursin("400", resp) && occursin("40-character", resp)
        resp = BuildRequest.handle_event(
            "{\"repo\":\"Someone/else\",\"sha\":\"1234567890abcdef1234567890abcdef12345678\"}", lctx)
        @test occursin("403", resp)

        # --- Buildkite state polling: failed builds become build-failed answers ---
        import HTTP as BkHTTP
        bk_state = Ref("running")
        triggers = Ref(0)               # own-pipeline builds we actually fired
        upstream_state = Ref("none")    # julia-pr's build of the commit, if any
        bk_router = BkHTTP.Router()
        BkHTTP.register!(bk_router, "POST", "/v2/organizations/testorg/pipelines/testpipe/builds",
            req -> begin
                triggers[] += 1
                BkHTTP.Response(201, JSON.json(Dict(
                    "number" => 42, "state" => "scheduled", "web_url" => "http://bk/42")))
            end)
        BkHTTP.register!(bk_router, "GET", "/v2/organizations/testorg/pipelines/julia-pr/builds",
            req -> BkHTTP.Response(200, upstream_state[] == "none" ? "[]" :
                JSON.json([Dict("number" => 7, "state" => upstream_state[],
                                "web_url" => "http://bk/up/7")])))
        BkHTTP.register!(bk_router, "GET", "/v2/organizations/testorg/pipelines/julia-pr/builds/7",
            req -> BkHTTP.Response(200, JSON.json(Dict(
                "number" => 7, "state" => upstream_state[], "web_url" => "http://bk/up/7"))))
        BkHTTP.register!(bk_router, "GET", "/v2/organizations/testorg/pipelines/testpipe/builds/42",
            req -> BkHTTP.Response(200, JSON.json(Dict(
                "number" => 42, "state" => bk_state[], "web_url" => "http://bk/42"))))
        bk_port, bk_server = let p = 0, srv = nothing
            for attempt in 1:10   # random ports collide occasionally; retry
                p = rand(40001:50000)
                try
                    srv = BkHTTP.serve!(bk_router, "127.0.0.1", p)
                    break
                catch err
                    attempt == 10 && rethrow()
                end
            end
            p, srv
        end
        SSM.put_parameter("/pkgeval/buildkite-token", "bk-test-token",
                          Dict("Type" => "SecureString", "Overwrite" => true); aws_config=aws)
        setat(k, t) = Dynamodb.update_item(  # backdate a claim
            Dict("build_key" => Dict("S" => k)), "pkgeval-builds",
            Dict("UpdateExpression" => "SET requested_at = :t",
                 "ExpressionAttributeValues" => Dict(":t" => Dict("S" => BuildRequest.isodate(t))));
            aws_config=aws)
        sha2 = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        key2 = "$sha2/linuxassert"
        ask = "{\"repo\":\"JuliaLang/julia\",\"sha\":\"$sha2\",\"variant\":\"linuxassert\"}"
        try
            with_env(Dict("PKGEVAL_BUILDS_TABLE" => "pkgeval-builds",
                          "BUILDKITE_API_BASE" => "http://127.0.0.1:$bk_port",
                          "BUILDKITE_ORG" => "testorg", "BUILDKITE_PIPELINE" => "testpipe",
                          "BUILDKITE_TOKEN_PARAM" => "/pkgeval/buildkite-token")) do
                # fresh ask: claims, triggers, records the build identity;
                # the answer links the triggered build
                resp = BuildRequest.handle_event(ask, lctx)
                @test occursin("\"statusCode\":202", resp) && occursin("requested", resp)
                @test occursin("http://bk/42", resp)
                claim = BuildRequest.get_claim(lctx, "pkgeval-builds", key2)
                @test FL.int(something(claim), "build_number", -1) == 42
                @test FL.str(something(claim), "build_url", "") == "http://bk/42"

                # build still running: dedup answer, still carrying the URL
                resp = BuildRequest.handle_event(ask, lctx)
                @test occursin("already-requested", resp) && occursin("http://bk/42", resp)

                # Buildkite reports failure: claim flips, answer carries the URL
                bk_state[] = "failed"
                resp = BuildRequest.handle_event(ask, lctx)
                @test occursin("build-failed", resp) && occursin("http://bk/42", resp)
                claim = BuildRequest.get_claim(lctx, "pkgeval-builds", key2)
                @test FL.str(something(claim), "status", "") == "failed"

                # failure is sticky: answered from the claim, no re-poll
                bk_state[] = "running"
                resp = BuildRequest.handle_event(ask, lctx)
                @test occursin("build-failed", resp)

                # ...until FAILED_RETRY_AGE passes, then an ask re-triggers
                setat(key2, Dates.now(UTC) - Dates.Hour(25))
                resp = BuildRequest.handle_event(ask, lctx)
                @test occursin("\"statusCode\":202", resp) && occursin("requested", resp)
                claim = BuildRequest.get_claim(lctx, "pkgeval-builds", key2)
                @test FL.str(something(claim), "status", "") == "requested"

                # age backstop: no artifact after MAX_BUILD_AGE means failed,
                # whatever Buildkite says about the build
                setat(key2, Dates.now(UTC) - Dates.Hour(4))
                resp = BuildRequest.handle_event(ask, lctx)
                @test occursin("build-failed", resp)

                # a claim without a recorded build identity still answers with
                # a link: the builds page filtered by commit
                sha3 = "ffffffffffffffffffffffffffffffffffffffff"
                @test BuildRequest.claim_build(lctx, "pkgeval-builds", "$sha3/linuxassert", "test")
                setat("$sha3/linuxassert", Dates.now(UTC) - Dates.Hour(4))
                resp = BuildRequest.handle_event(
                    "{\"repo\":\"JuliaLang/julia\",\"sha\":\"$sha3\",\"variant\":\"linuxassert\"}", lctx)
                @test occursin("build-failed", resp)
                @test occursin("buildkite.com/testorg/testpipe/builds?commit=$sha3", resp)
            end

            # --- waiting on upstream CI builds instead of duplicating them ---
            setattr(k, name, t) = Dynamodb.update_item(
                Dict("build_key" => Dict("S" => k)), "pkgeval-builds",
                Dict("UpdateExpression" => "SET #a = :t",
                     "ExpressionAttributeNames" => Dict("#a" => name),
                     "ExpressionAttributeValues" => Dict(":t" => Dict("S" => BuildRequest.isodate(t))));
                aws_config=aws)
            statusof(k) = FL.str(something(BuildRequest.get_claim(lctx, "pkgeval-builds", k)), "status", "")
            # the body is JSON-escaped inside the response envelope, so match
            # statuses positionally rather than with quoted-literal fragments
            is_requested(resp) = occursin("\"statusCode\":202", resp) &&
                occursin("requested", resp) && !occursin("already-requested", resp) &&
                !occursin("waiting-upstream", resp)
            with_env(Dict("PKGEVAL_BUILDS_TABLE" => "pkgeval-builds",
                          "BUILDKITE_API_BASE" => "http://127.0.0.1:$bk_port",
                          "BUILDKITE_ORG" => "testorg", "BUILDKITE_PIPELINE" => "testpipe",
                          "BUILDKITE_TOKEN_PARAM" => "/pkgeval/buildkite-token",
                          "BUILDKITE_UPSTREAM_PIPELINES" => "julia-pr")) do
                # upstream in flight: no duplicate trigger, claim waits on it
                upstream_state[] = "running"
                shaU = "1111111111111111111111111111111111111112"
                askU = "{\"repo\":\"JuliaLang/julia\",\"sha\":\"$shaU\",\"variant\":\"linuxassert\"}"
                n0 = triggers[]
                resp = BuildRequest.handle_event(askU, lctx)
                @test occursin("waiting-upstream", resp)
                @test occursin("http://bk/up/7", resp)   # links the upstream build
                @test triggers[] == n0
                @test statusof("$shaU/linuxassert") == "upstream"
                # still in flight on re-ask: keep waiting, still no trigger
                resp = BuildRequest.handle_event(askU, lctx)
                @test occursin("already-requested", resp)
                @test triggers[] == n0

                # upstream turns terminal: first observation starts the grace
                # clock; once it lapses (artifact never appeared), we trigger
                # our own build — never build-failed from an upstream failure
                upstream_state[] = "failed"
                resp = BuildRequest.handle_event(askU, lctx)
                @test occursin("already-requested", resp)
                @test triggers[] == n0
                setattr("$shaU/linuxassert", "upstream_done_at",
                        Dates.now(UTC) - Dates.Minute(11))
                resp = BuildRequest.handle_event(askU, lctx)
                @test is_requested(resp)
                @test triggers[] == n0 + 1
                @test statusof("$shaU/linuxassert") == "requested"
                claim = something(BuildRequest.get_claim(lctx, "pkgeval-builds", "$shaU/linuxassert"))
                @test FL.int(claim, "build_number", -1) == 42

                # upstream that recently *passed* while workers still ask
                # (artifact upload lag): wait out the grace, then trigger
                upstream_state[] = "passed"
                shaP = "1111111111111111111111111111111111111113"
                askP = "{\"repo\":\"JuliaLang/julia\",\"sha\":\"$shaP\",\"variant\":\"linuxassert\"}"
                nP = triggers[]
                resp = BuildRequest.handle_event(askP, lctx)
                @test occursin("waiting-upstream", resp)
                @test triggers[] == nP
                resp = BuildRequest.handle_event(askP, lctx)   # inside grace
                @test occursin("already-requested", resp)
                setattr("$shaP/linuxassert", "upstream_done_at",
                        Dates.now(UTC) - Dates.Minute(11))
                resp = BuildRequest.handle_event(askP, lctx)
                @test is_requested(resp)
                @test triggers[] == nP + 1

                # no upstream build at all: trigger immediately, as ever
                upstream_state[] = "none"
                shaN = "1111111111111111111111111111111111111114"
                resp = BuildRequest.handle_event(
                    "{\"repo\":\"JuliaLang/julia\",\"sha\":\"$shaN\",\"variant\":\"linuxassert\"}", lctx)
                @test is_requested(resp)
                @test statusof("$shaN/linuxassert") == "requested"

                # upstream stuck in flight past the age backstop: take over
                # with our own build rather than declaring failure
                upstream_state[] = "running"
                shaS = "1111111111111111111111111111111111111115"
                askS = "{\"repo\":\"JuliaLang/julia\",\"sha\":\"$shaS\",\"variant\":\"linuxassert\"}"
                resp = BuildRequest.handle_event(askS, lctx)
                @test occursin("waiting-upstream", resp)
                setat("$shaS/linuxassert", Dates.now(UTC) - Dates.Hour(4))
                nS = triggers[]
                resp = BuildRequest.handle_event(askS, lctx)
                @test is_requested(resp)
                @test triggers[] == nS + 1
                @test statusof("$shaS/linuxassert") == "requested"
            end
        finally
            close(bk_server)
        end
    end

    @testset "fleet drain (scale-in protection)" begin
        # a two-instance ASG in moto, with this "worker" playing the newest one
        Auto_Scaling.create_launch_configuration("pkgeval-lc",
            Dict{String,Any}("ImageId" => "ami-12345678", "InstanceType" => "m5.large");
            aws_config=aws)
        Auto_Scaling.create_auto_scaling_group("pkgeval-test-asg", 4, 0, Dict{String,Any}(
            "DesiredCapacity" => 2, "LaunchConfigurationName" => "pkgeval-lc",
            "AvailabilityZones" => ["us-east-1a"],
            "NewInstancesProtectedFromScaleIn" => true); aws_config=aws)
        resp = Auto_Scaling.describe_auto_scaling_groups(
            Dict{String,Any}("AutoScalingGroupNames" => ["pkgeval-test-asg"]); aws_config=aws)
        group = resp["DescribeAutoScalingGroupsResult"]["AutoScalingGroups"]["member"]
        insts = group["Instances"]["member"]
        ids = sort!([String(m["InstanceId"]) for m in insts])
        @test length(ids) == 2

        protected(id) = begin
            r = Auto_Scaling.describe_auto_scaling_groups(
                Dict{String,Any}("AutoScalingGroupNames" => ["pkgeval-test-asg"]); aws_config=aws)
            ms = r["DescribeAutoScalingGroupsResult"]["AutoScalingGroups"]["member"]["Instances"]["member"]
            only(filter(m -> m["InstanceId"] == id, ms))["ProtectedFromScaleIn"] == "true"
        end

        @test PEF.drain_decision(0, 32)             # 32 slots ahead, empty queue
        @test PEF.drain_decision(0, 0)              # empty queue: even the senior lets go
        @test !PEF.drain_decision(1, 0)             # any work: the most senior holds on
        @test !PEF.drain_decision(64, 32)           # plenty of work: keep claiming
        @test PEF.drain_decision(31, 32)

        # size-aware slot accounting: type-derived, with the ASG's weighted
        # capacity taking precedence when reported
        @test PEF.instance_slots("m6a.8xlarge", 32) == 32
        @test PEF.instance_slots("m6a.24xlarge", 32) == 96
        @test PEF.instance_slots("m5a.xlarge", 32) == 4
        @test PEF.instance_slots("c6a.large", 32) == 2
        @test PEF.instance_slots("u-6tb1.metal", 32) == 32  # unknown: assume own shape
        @test PEF.member_slots(Dict("WeightedCapacity" => "96", "InstanceType" => "m6a.8xlarge"), 32) == 96
        @test PEF.member_slots(Dict("InstanceType" => "m6a.16xlarge"), 32) == 64
        @test PEF.member_slots(Dict{String,Any}(), 32) == 32

        fleet = PEF.FleetDrain(; asg="pkgeval-test-asg", instance_id=last(ids), slots=32)
        # newest of the two m5.large members: one 2-slot instance ranked ahead
        @test PEF.fleet_standing(ctx, fleet) == (2, 2)

        # fleet sizing for the fast/slow cutoff: env override > ASG (in-service
        # slots beat the nominal desired capacity) > static default
        withenv("PKGEVAL_ASG_NAME" => "pkgeval-test-asg", "PKGEVAL_FLEET_SLOTS" => nothing) do
            @test PEF.live_fleet_slots(ctx) == 4    # two m5.large in service
        end
        withenv("PKGEVAL_ASG_NAME" => "pkgeval-test-asg", "PKGEVAL_FLEET_SLOTS" => "999") do
            @test PEF.live_fleet_slots(ctx) == 999
        end
        withenv("PKGEVAL_ASG_NAME" => nothing, "PKGEVAL_FLEET_SLOTS" => nothing) do
            @test PEF.live_fleet_slots(ctx) == 128
        end

        # empty queues + no running jobs => drain and unprotect
        @test PEF.pause_claiming!(ctx, fleet, 0)
        @test !protected(last(ids))
        @test protected(first(ids))                 # only ourselves, never others

        # work appears => resume and re-protect (reset the check throttle)
        PEF.enqueue_jobs(ctx, [PEF.JobRef("nonexistent-run", "primary", "P$i") for i in 1:40])
        fleet.last_check = 0.0
        @test !PEF.pause_claiming!(ctx, fleet, 0)
        @test protected(last(ids))

        # busy instance never unprotects, even while told to drain
        for _ in 1:40   # drop the fake messages (claim_job swallows them)
            PEF.claim_job(ctx; wait=0) === nothing || break
        end
        fleet.last_check = 0.0
        @test PEF.pause_claiming!(ctx, fleet, 3)    # draining again (queue empty)...
        @test protected(last(ids))                  # ...but 3 jobs still running
    end

    @testset "DLQ consumer closes out dead jobs and runs" begin
        # a run with two jobs; one completes normally, one dies to the DLQ
        run_id = PEF.create_run(ctx, PEF.RunSpec(configs[1:1], ["Alive", "Dead"], Dict{String,Any}());
                                submitter="tester", reuse=false)
        expand = nothing
        for _ in 1:10
            expand = PEF.claim_job(ctx; wait=1)
            expand isa PEF.ClaimedExpand && break
        end
        @test PEF.expand_run(ctx, run_id, ["Alive", "Dead"]) == 2
        SQS.delete_message(expand.queue_url, expand.receipt_handle; aws_config=aws)
        for _ in 1:6
            c = PEF.claim_job(ctx; wait=1)
            c isa PEF.ClaimedJob || continue
            if c.job.package == "Alive"
                PEF.record_result(ctx, c, PEF.JobResult(; status="test", duration=1.0))
            else
                PEF.release_job(ctx, c; delay=3600)  # never completes
            end
        end

        # what Lambda delivers when the Dead job's message hits the DLQ
        sqs_event(bodies) = JSON.json(Dict("Records" => [Dict("body" => b) for b in bodies]))
        gh_stub = PEF.FarmLite.GitHubCtx("unused", "http://127.0.0.1:1")  # must not be contacted
        lctx2 = PEF.FarmLite.LiteCtx(; region="us-east-1",
                    creds=PEF.FarmLite.AwsCreds("testing", "testing", nothing),
                    queue_url, slow_queue_url, runs_table="pkgeval-runs",
                    jobs_table="pkgeval-jobs", bucket="pkgeval-results",
                    endpoint="http://127.0.0.1:$port")
        resp = PEF.FarmBot.handle_event(
            sqs_event(["{\"run_id\":\"$run_id\",\"config\":\"primary\",\"package\":\"Dead\"}"]),
            lctx2, gh_stub)
        @test occursin("ok", resp)

        jobs = PEF.run_jobs(ctx, run_id)
        dead = only(filter(j -> j["package"] == "Dead", jobs))
        @test dead["status"] == "error"
        @test dead["reason"] == "undeliverable"
        run = PEF.get_run(ctx, run_id)
        @test run["status"] == "done"          # the dead job was the last one
        @test run["completed_jobs"] == 2

        # double delivery of the same dead message must not double-count
        PEF.FarmBot.handle_event(
            sqs_event(["{\"run_id\":\"$run_id\",\"config\":\"primary\",\"package\":\"Dead\"}"]),
            lctx2, gh_stub)
        @test PEF.get_run(ctx, run_id)["completed_jobs"] == 2

        # a dead message whose Julia build is still pending gets RECYCLED:
        # re-enqueued with a fresh receive budget, job untouched
        shacfg = [PkgEval.Configuration(; name="primary",
                      julia="JuliaLang/julia#1234567890abcdef1234567890abcdef12345678")]
        run3 = PEF.create_run(ctx, PEF.RunSpec(shacfg, ["Waiting"], Dict{String,Any}());
                              submitter="tester", reuse=false)
        expand3 = nothing
        for _ in 1:10
            expand3 = PEF.claim_job(ctx; wait=1)
            expand3 isa PEF.ClaimedExpand && break
        end
        PEF.expand_run(ctx, run3, ["Waiting"])
        SQS.delete_message(expand3.queue_url, expand3.receipt_handle; aws_config=aws)
        c3 = PEF.claim_job(ctx; wait=1)   # take the job in flight (like a worker would)
        @test c3 isa PEF.ClaimedJob

        # a fresh build claim => the dead message must be recycled, not buried
        Dynamodb.put_item(Dict(
            "build_key" => Dict("S" => "1234567890abcdef1234567890abcdef12345678/linux"),
            "requested_at" => Dict("S" => PEF.FarmBot.isodate()),
            "status" => Dict("S" => "requested")), "pkgeval-builds"; aws_config=aws)
        withenv("PKGEVAL_BUILDS_TABLE" => "pkgeval-builds") do
            PEF.FarmBot.handle_event(
                sqs_event(["{\"run_id\":\"$run3\",\"config\":\"primary\",\"package\":\"Waiting\"}"]),
                lctx2, gh_stub)
        end
        job3 = only(PEF.run_jobs(ctx, run3))
        @test job3["status"] == "running"              # untouched, not errored
        @test PEF.get_run(ctx, run3)["status"] == "active"
        # in the real lifecycle the worker's last build-wait re-ask RELEASED
        # the claim (clearing the liveness stamp) before the message died to
        # the DLQ — a message only dead-letters with no live holder. Without
        # the release, the recycled message must bounce off the fresh stamp.
        @test PEF.claim_job(ctx; wait=1) === nothing   # live claim: dup dropped
        PEF.release_job(ctx, c3; delay=0)
        withenv("PKGEVAL_BUILDS_TABLE" => "pkgeval-builds") do
            PEF.FarmBot.handle_event(
                sqs_event(["{\"run_id\":\"$run3\",\"config\":\"primary\",\"package\":\"Waiting\"}"]),
                lctx2, gh_stub)
        end
        recycled = PEF.claim_job(ctx; wait=1)          # the re-enqueued message
        @test recycled isa PEF.ClaimedJob
        @test recycled.job.package == "Waiting"
        PEF.record_result(ctx, recycled, PEF.JobResult(; status="test", duration=1.0))
        for _ in 1:4                                    # swallow the stale duplicate
            PEF.claim_job(ctx; wait=1) === nothing && break
        end

        # a stale claim (old requested_at) no longer shields the message
        Dynamodb.put_item(Dict(
            "build_key" => Dict("S" => "1234567890abcdef1234567890abcdef12345678/linux"),
            "requested_at" => Dict("S" => "2020-01-01T00:00:00Z"),
            "status" => Dict("S" => "requested")), "pkgeval-builds"; aws_config=aws)
        run4 = PEF.create_run(ctx, PEF.RunSpec(shacfg, ["Doomed"], Dict{String,Any}());
                              submitter="tester", reuse=false)
        expand4 = nothing
        for _ in 1:10
            expand4 = PEF.claim_job(ctx; wait=1)
            expand4 isa PEF.ClaimedExpand && break
        end
        PEF.expand_run(ctx, run4, ["Doomed"])
        SQS.delete_message(expand4.queue_url, expand4.receipt_handle; aws_config=aws)
        withenv("PKGEVAL_BUILDS_TABLE" => "pkgeval-builds") do
            PEF.FarmBot.handle_event(
                sqs_event(["{\"run_id\":\"$run4\",\"config\":\"primary\",\"package\":\"Doomed\"}"]),
                lctx2, gh_stub)
        end
        @test only(PEF.run_jobs(ctx, run4))["status"] == "error"
        @test PEF.get_run(ctx, run4)["status"] == "done"
        for _ in 1:4                                    # drain run4's job message
            PEF.claim_job(ctx; wait=1) === nothing && break
        end

        # a dead *expand* message fails the whole run
        run2 = PEF.create_run(ctx, PEF.RunSpec(configs[1:1], String[], Dict{String,Any}());
                              submitter="tester", reuse=false)
        PEF.FarmBot.handle_event(sqs_event(["{\"run_id\":\"$run2\",\"expand\":true}"]),
                                 lctx2, gh_stub)
        @test PEF.get_run(ctx, run2)["status"] == "failed"

        # drain run2's expand message so later testsets start clean
        for _ in 1:6
            c = PEF.claim_job(ctx; wait=1)
            c === nothing && break
            c isa PEF.ClaimedExpand && SQS.delete_message(c.queue_url, c.receipt_handle; aws_config=aws)
        end
    end

    @testset "fleet generation heartbeat" begin
        # no record => the heartbeat must not create one (cloud-init owns creation)
        PEF.heartbeat_generation(ctx)
        resp = Dynamodb.get_item(PEF.ddb_item(Dict("run_id" => "_fleet-generation")),
                                 "pkgeval-runs"; aws_config=aws)
        @test !haskey(resp, "Item")

        # with a record, the beat refreshes the timestamp
        Dynamodb.put_item(PEF.ddb_item(Dict("run_id" => "_fleet-generation",
                                            "ref" => "abc", "heartbeat_at" => "old")),
                          "pkgeval-runs"; aws_config=aws)
        PEF.heartbeat_generation(ctx)
        resp = Dynamodb.get_item(PEF.ddb_item(Dict("run_id" => "_fleet-generation")),
                                 "pkgeval-runs"; aws_config=aws)
        beat = PEF.ddb_parse(resp["Item"])["heartbeat_at"]
        @test beat != "old" && occursin("T", beat)

        # the sentinel must be invisible to the donor/report scans
        @test all(t -> t[2] != "_fleet-generation", PEF.completed_runs(ctx))
        Dynamodb.delete_item(PEF.ddb_item(Dict("run_id" => "_fleet-generation")),
                             "pkgeval-runs"; aws_config=aws)
    end

    @testset "sealed compilecache" begin
        seal_q = SQS.create_queue("pkgeval-jobs-seal"; aws_config=aws)["QueueUrl"]
        deriv_q = SQS.create_queue("pkgeval-jobs-deriv"; aws_config=aws)["QueueUrl"]
        scfg = FarmConfig(; region="us-east-1", queue_url, slow_queue_url,
                          seal_queue_url=seal_q, deriv_queue_url=deriv_q,
                          runs_table="pkgeval-runs",
                          jobs_table="pkgeval-jobs", bucket="pkgeval-results")
        sctx = FarmCtx(scfg, aws)

        # fixture registry: JSON -> Crayons, Example and Crayons are leaves
        reg = mktempdir()
        mkpath(joinpath(reg, "J", "JSON"))
        write(joinpath(reg, "Registry.toml"), """
            [packages]
            aaaaaaaa-0000-0000-0000-000000000001 = { name = "Example", path = "E/Example" }
            aaaaaaaa-0000-0000-0000-000000000002 = { name = "Crayons", path = "C/Crayons" }
            aaaaaaaa-0000-0000-0000-000000000003 = { name = "JSON", path = "J/JSON" }
            """)
        write(joinpath(reg, "J", "JSON", "Deps.toml"), """
            ["0"]
            Crayons = "aaaaaaaa-0000-0000-0000-000000000002"
            """)
        PEF.SEAL_REGISTRY_OVERRIDE[] = reg
        PEF.SEAL_SUPPORT_OVERRIDE[] = true   # no sandbox here to detect with
        cache_dir = mktempdir()

        withenv("PKGEVAL_SEAL_CACHE" => cache_dir) do
            packages = ["Example", "Crayons", "JSON"]
            run_id = PEF.create_run(sctx, PEF.RunSpec(configs, packages, Dict{String,Any}());
                                    submitter="tester", reuse=false)
            expand = nothing
            for _ in 1:10
                expand = PEF.claim_job(sctx; wait=1)
                expand isa PEF.ClaimedExpand && break
            end
            @test expand isa PEF.ClaimedExpand
            @test PEF.expand_run(sctx, run_id, packages) == 6
            SQS.delete_message(expand.queue_url, expand.receipt_handle; aws_config=aws)

            # expansion recorded the config -> seal-run mapping (one per julia)
            run = PEF.get_run(sctx, run_id)
            seal_runs = run["seal_runs"]
            @test length(seal_runs) == 2
            @test seal_runs["primary"] != seal_runs["against"]
            sr = seal_runs["primary"]
            @test startswith(sr, "seal-")

            # seal jobs exist with counters: leaves ready, JSON gated on Crayons
            seal_jobs = Dict(j["package"] => j for j in PEF.run_jobs(sctx, sr))
            @test length(seal_jobs) == 3
            @test seal_jobs["JSON"]["remaining"] == 1
            @test seal_jobs["JSON"]["deps"] == ["Crayons"]
            @test seal_jobs["Crayons"]["remaining"] == 0
            @test "JSON" in seal_jobs["Crayons"]["dependents"]
            @test PEF.get_run(sctx, sr)["total_jobs"] == 3

            # workers prefer the seal queue: the next claims are seal jobs (the
            # ready leaves of both seal runs), before any of the 6 test jobs
            gated_state, gated_run = PEF.seal_state(sctx, run,
                                                    PEF.JobRef(run_id, "primary", "JSON"))
            @test gated_state == :pending && gated_run == sr

            claimed_seals = 0
            for _ in 1:12
                c = PEF.claim_job(sctx; wait=1)
                c isa PEF.ClaimedJob || continue
                if PEF.is_seal_job(c.job)
                    claimed_seals += 1
                    # complete it the way process_seal_job does, sans evaluation
                    PEF.record_result(sctx, c, PEF.JobResult(; status="sealed", duration=1.0))
                    item = PEF.get_seal_item(sctx, c.job)
                    deps = unique(String.(get(item, "dependents", String[])))
                    isempty(deps) || PEF.propagate_seal_completion(sctx, c.job.run_id, deps)
                else
                    # a test job surfaced: only legal once its seal queue is empty;
                    # release it and keep going
                    PEF.release_job(sctx, c; delay=0)
                end
                claimed_seals == 4 && break
            end
            @test claimed_seals == 4   # Example+Crayons for both seal runs

            # Crayons' completion decremented and enqueued JSON in both seal runs
            @test PEF.ddb_parse(Dynamodb.get_item(
                PEF.ddb_item(Dict("run_id" => sr, "job_key" => "seal#JSON")),
                scfg.jobs_table; aws_config=aws)["Item"])["remaining"] == 0
            for _ in 1:8
                c = PEF.claim_job(sctx; wait=1)
                c isa PEF.ClaimedJob || continue
                if PEF.is_seal_job(c.job)
                    @test c.job.package == "JSON"
                    PEF.record_result(sctx, c, PEF.JobResult(; status="sealed", duration=1.0))
                else
                    PEF.release_job(sctx, c; delay=0)
                end
            end

            # both seal runs completed and flipped done; the gate reads terminal
            @test PEF.get_run(sctx, sr)["status"] == "done"
            @test PEF.seal_status(sctx, sr, "JSON") == :terminal
            @test PEF.seal_state(sctx, run, PEF.JobRef(run_id, "primary", "JSON")) ==
                  (:terminal, sr)
            # absent seal job (package never sealed) -> :none, run immediately
            @test PEF.seal_status(sctx, sr, "NeverSealed") == :none

            # publication primitive: create-only with checksum comparison —
            # identical republish is a no-op, different content is detected
            let sid = PEF.seal_id_of(sr)
                @test PEF.put_sealed_object(sctx, "compilecache/$sid/probe",
                                            Vector{UInt8}("x")) == :created
                @test PEF.put_sealed_object(sctx, "compilecache/$sid/probe",
                                            Vector{UInt8}("x")) in (:exists_same, :exists_unknown)
                outcome = PEF.put_sealed_object(sctx, "compilecache/$sid/probe",
                                                Vector{UInt8}("y"))
                @test outcome in (:exists_differs, :exists_unknown)
                outcome == :exists_unknown &&
                    @test_skip "moto build doesn't return checksums"
            end

            # reconciliation: a stuck counter (deps all terminal) is healed and
            # a ready-but-unclaimed job re-enqueued
            Dynamodb.update_item(
                PEF.ddb_item(Dict("run_id" => sr, "job_key" => "seal#JSON")),
                scfg.jobs_table,
                Dict("UpdateExpression" => "SET #s = :p, remaining = :r",
                     "ExpressionAttributeNames" => Dict("#s" => "status"),
                     "ExpressionAttributeValues" => PEF.ddb_item(Dict(":p" => "pending", ":r" => 1)));
                aws_config=aws)
            healed = PEF.reconcile_seal_run(sctx, sr)
            @test healed == ["JSON"]
            c = PEF.claim_job(sctx; wait=1)
            @test c isa PEF.ClaimedJob && c.job.package == "JSON" && PEF.is_seal_job(c.job)
            PEF.record_result(sctx, c, PEF.JobResult(; status="unsealable",
                                                     reason="precompile", duration=1.0))

            # cycle amnesty: a pending component that only waits on itself
            # (items predating seal_dep_graph's cycle collapse — the
            # Compat <-> SHA deadlock that froze every ecosystem seal run) is
            # released whole by reconcile; components with a live external dep
            # are left to normal decrement propagation
            cyc_run = "seal-cycleamnesty00"
            Dynamodb.put_item(PEF.ddb_item(Dict(
                    "run_id" => cyc_run, "created_at" => PEF.isodate(),
                    "submitter" => "sealing", "status" => "active", "kind" => "seal",
                    "configs" => "[]", "packages" => "[]", "context" => "{}",
                    "total_jobs" => 5, "completed_jobs" => 1)),
                "pkgeval-runs"; aws_config=aws)
            for (pkg, dep) in (("CycA", "CycB"), ("CycB", "CycA"))
                Dynamodb.put_item(PEF.ddb_item(Dict(
                        "run_id" => cyc_run, "job_key" => PEF.job_key(PEF.SEAL_CONFIG_NAME, pkg),
                        "config" => PEF.SEAL_CONFIG_NAME, "package" => pkg, "kind" => "seal",
                        "status" => "pending", "attempts" => 0,
                        "deps" => [dep, "CycDone"], "dependents" => [dep],
                        "remaining" => 2)), "pkgeval-jobs"; aws_config=aws)
            end
            Dynamodb.put_item(PEF.ddb_item(Dict(
                    "run_id" => cyc_run, "job_key" => PEF.job_key(PEF.SEAL_CONFIG_NAME, "CycDone"),
                    "config" => PEF.SEAL_CONFIG_NAME, "package" => "CycDone", "kind" => "seal",
                    "status" => "sealed", "attempts" => 1,
                    "deps" => Any[], "dependents" => Any[], "remaining" => 0)),
                "pkgeval-jobs"; aws_config=aws)
            # a second cycle blocked on a genuinely running dep must NOT release
            for (pkg, dep) in (("HeldA", "HeldB"), ("HeldB", "HeldA"))
                Dynamodb.put_item(PEF.ddb_item(Dict(
                        "run_id" => cyc_run, "job_key" => PEF.job_key(PEF.SEAL_CONFIG_NAME, pkg),
                        "config" => PEF.SEAL_CONFIG_NAME, "package" => pkg, "kind" => "seal",
                        "status" => "pending", "attempts" => 0,
                        "deps" => [dep, "CycLive"], "dependents" => [dep],
                        "remaining" => 2)), "pkgeval-jobs"; aws_config=aws)
            end
            Dynamodb.put_item(PEF.ddb_item(Dict(
                    "run_id" => cyc_run, "job_key" => PEF.job_key(PEF.SEAL_CONFIG_NAME, "CycLive"),
                    "config" => PEF.SEAL_CONFIG_NAME, "package" => "CycLive", "kind" => "seal",
                    "status" => "running", "attempts" => 1,
                    "deps" => Any[], "dependents" => ["HeldA", "HeldB"], "remaining" => 0)),
                "pkgeval-jobs"; aws_config=aws)
            @test sort(PEF.reconcile_seal_run(sctx, cyc_run)) == ["CycA", "CycB"]
            for pkg in ("CycA", "CycB")
                item = PEF.get_seal_item(sctx, PEF.JobRef(cyc_run, PEF.SEAL_CONFIG_NAME, pkg))
                @test item["remaining"] == 0 && item["status"] == "pending"
            end
            @test PEF.get_seal_item(sctx, PEF.JobRef(cyc_run, PEF.SEAL_CONFIG_NAME, "HeldA"))["remaining"] == 2
            for _ in 1:3   # drain the two released messages
                c2 = PEF.claim_job(sctx; wait=1)
                c2 === nothing && break
                if c2 isa PEF.ClaimedJob && c2.job.run_id == cyc_run
                    PEF.record_result(sctx, c2, PEF.JobResult(; status="sealed", duration=0.1))
                else
                    PEF.release_job(sctx, c2; delay=0)
                end
            end

            # learned edges round-trip
            PEF.record_learned_edges(sctx, "JSON", ["Crayons", "TestExtras"])
            @test PEF.learned_edges(sctx)["JSON"] == ["Crayons", "TestExtras"]

            # augmentation: a second run sharing the config adds only missing
            # packages, and completed deps count as satisfied
            graph2 = Dict("NewPkg" => ["Crayons"], "Crayons" => String[])
            ncreated, ready = PEF.add_seal_jobs(sctx, sr, ["NewPkg", "Crayons"], graph2)
            @test ncreated == 1
            @test "NewPkg" in ready          # Crayons already terminal
            @test PEF.get_run(sctx, sr)["status"] == "active"   # done -> reactivated
            @test PEF.get_run(sctx, sr)["total_jobs"] == 4

            # drain the queues so later testsets start clean
            for _ in 1:20
                c = PEF.claim_job(sctx; wait=1)
                c === nothing && break
                c isa PEF.ClaimedJob || continue
                PEF.record_result(sctx, c, PEF.JobResult(; status=PEF.is_seal_job(c.job) ?
                                                          "sealed" : "test", duration=1.0))
            end
        end
        # scheme symmetry (#12): a definitive "no hook" verdict for ANY config
        # runs the whole run cold — no partial seal_runs map that would compare
        # a sealed julia against a cold one
        PEF.SEAL_SUPPORT_OVERRIDE[] = nothing
        let cfgs = [Dict{String,Any}("name" => "primary", "julia" => "fake-julia-A"),
                    Dict{String,Any}("name" => "against", "julia" => "fake-julia-B")],
            run_dict = Dict{String,Any}("run_id" => "symm-test", "configs" => cfgs),
            fresh = [PEF.JobRef("symm-test", "primary", "JSON"),
                     PEF.JobRef("symm-test", "against", "JSON")],
            fpA = PEF.seal_fingerprint(cfgs[1]), fpB = PEF.seal_fingerprint(cfgs[2])

            PEF.SEAL_SUPPORT_CACHE[fpA] = true
            PEF.SEAL_SUPPORT_CACHE[fpB] = false
            @test PEF.setup_sealing(sctx, run_dict, fresh) === nothing
            # both definitively hooked: both configs seal
            PEF.SEAL_SUPPORT_CACHE[fpB] = true
            mapping = PEF.setup_sealing(sctx, run_dict, fresh)
            @test mapping !== nothing && length(mapping) == 2
            delete!(PEF.SEAL_SUPPORT_CACHE, fpA)
            delete!(PEF.SEAL_SUPPORT_CACHE, fpB)
            # close out the seal jobs this enqueued so later testsets start clean
            for _ in 1:6
                c = PEF.claim_job(sctx; wait=1)
                c === nothing && break
                c isa PEF.ClaimedJob &&
                    PEF.record_result(sctx, c, PEF.JobResult(; status="sealed", duration=0.1))
            end
        end
        PEF.SEAL_REGISTRY_OVERRIDE[] = nothing
        PEF.SEAL_SUPPORT_OVERRIDE[] = nothing
    end

    @testset "cache-protocol scheme (proxy + namespaced publication)" begin
        import HTTP as ProxyHTTP
        # same physical queue as the sealing testset (create_queue is idempotent):
        # want ingestion enqueues real derivation messages
        seal_q = SQS.create_queue("pkgeval-jobs-seal"; aws_config=aws)["QueueUrl"]
        deriv_q = SQS.create_queue("pkgeval-jobs-deriv"; aws_config=aws)["QueueUrl"]
        scfg = FarmConfig(; region="us-east-1", queue_url, slow_queue_url,
                          seal_queue_url=seal_q, deriv_queue_url=deriv_q,
                          runs_table="pkgeval-runs",
                          jobs_table="pkgeval-jobs", bucket="pkgeval-results")
        sctx = FarmCtx(scfg, aws)
        cache_dir = mktempdir()
        withenv("PKGEVAL_SEAL_CACHE" => cache_dir) do
            # scheme is a per-seal-run property; anything but "protocol"
            # (retired schemes, pre-scheme runs) gates nothing
            @test PEF.seal_run_scheme(Dict{String,Any}("scheme" => "protocol")) == "protocol"
            @test PEF.seal_run_scheme(Dict{String,Any}()) != "protocol"
            proxy = PEF.start_seal_proxy!(sctx)
            try
                base = "http://127.0.0.1:$(proxy.port)"
                ns, uuid = "deadbeef", "aaaaaaaa-0000-0000-0000-000000000003"
                key = "ab" ^ 32

                # publication from a fixture export, only under the registry
                # uuid and only files inside the unit's own cache dir
                mktempdir() do export_dir
                    mkpath(joinpath(export_dir, "compiled", "v1.13", "JSON"))
                    write(joinpath(export_dir, "compiled", "v1.13", "JSON", "JSON_k.ji"), "JI")
                    open(joinpath(export_dir, "seal_keys.toml"), "w") do io
                        println(io, """
                            [JSON]
                            uuid = "$uuid"
                            key = "$key"
                            preimage = "v1"
                            ji = "v1.13/JSON/JSON_k.ji"
                            so = ""
                            """)
                    end
                    @test PEF.publish_protocol!(sctx, ns, export_dir, "JSON", uuid)

                    # a claimed foreign uuid is refused
                    @test !PEF.publish_protocol!(sctx, ns, export_dir, "JSON",
                                                 "bbbbbbbb-0000-0000-0000-000000000009")

                    # a path outside the unit's cache dir is refused
                    open(joinpath(export_dir, "seal_keys.toml"), "w") do io
                        println(io, """
                            [JSON]
                            uuid = "$uuid"
                            key = "$("cd"^32)"
                            preimage = "v1"
                            ji = "v1.13/Other/JSON_k.ji"
                            so = ""
                            """)
                    end
                    @test !PEF.publish_protocol!(sctx, ns, export_dir, "JSON", uuid)
                end

                # the unit's extensions publish under uuid5-derived authority;
                # a free-floating ext uuid claim is skipped, not published
                ext_uuid = string(uuid5(UUID(uuid), "JSONFooExt"))
                ext_key, bad_key = "ee"^32, "ff"^32
                mktempdir() do export_dir
                    mkpath(joinpath(export_dir, "compiled", "v1.13", "JSON"))
                    mkpath(joinpath(export_dir, "compiled", "v1.13", "JSONFooExt"))
                    mkpath(joinpath(export_dir, "compiled", "v1.13", "JSONBadExt"))
                    write(joinpath(export_dir, "compiled", "v1.13", "JSON", "JSON_k.ji"), "JI")
                    write(joinpath(export_dir, "compiled", "v1.13", "JSONFooExt", "JSONFooExt_k.ji"), "EXTJI")
                    write(joinpath(export_dir, "compiled", "v1.13", "JSONBadExt", "JSONBadExt_k.ji"), "BADJI")
                    open(joinpath(export_dir, "seal_keys.toml"), "w") do io
                        println(io, """
                            [JSON]
                            uuid = "$uuid"
                            key = "$key"
                            preimage = "v1"
                            ji = "v1.13/JSON/JSON_k.ji"
                            so = ""

                            [JSONFooExt]
                            uuid = "$ext_uuid"
                            ext_of = "$uuid"
                            key = "$ext_key"
                            preimage = "v3"
                            ji = "v1.13/JSONFooExt/JSONFooExt_k.ji"
                            so = ""

                            [JSONBadExt]
                            uuid = "cccccccc-0000-0000-0000-000000000001"
                            ext_of = "$uuid"
                            key = "$bad_key"
                            preimage = "v3"
                            ji = "v1.13/JSONBadExt/JSONBadExt_k.ji"
                            so = ""
                            """)
                    end
                    @test PEF.publish_protocol!(sctx, ns, export_dir, "JSON", uuid)
                    @test PEF.get_kv(sctx, ns, ext_uuid, ext_key) !== nothing
                    ext_meta = JSON.parse(String(PEF.get_kv(sctx, ns, ext_uuid, ext_key; meta=true)))
                    @test ext_meta["ext_of"] == uuid
                    @test PEF.get_kv(sctx, ns, "cccccccc-0000-0000-0000-000000000001", bad_key) === nothing
                end

                # the proxy serves the framed pair (S3-backed, then local cache)
                resp = ProxyHTTP.get("$base/cache/v1/$ns/$uuid/$key"; status_exception=false)
                @test resp.status == 200
                payload = resp.body
                len_ji = Int(ltoh(reinterpret(UInt64, payload[1:8])[1]))
                @test String(payload[9:8+len_ji]) == "JI"
                @test Int(ltoh(reinterpret(UInt64, payload[9+len_ji:16+len_ji])[1])) == 0
                @test isfile(joinpath(cache_dir, ns, "kv", uuid, key))   # cached locally

                # misses 404; wants are collected with their full context
                @test ProxyHTTP.get("$base/cache/v1/$ns/$uuid/$("ee"^32)";
                                    status_exception=false).status == 404
                @test ProxyHTTP.post("$base/want/v1"; body="v1\nuuid=$uuid",
                                     status_exception=false).status == 202
                @test any(w -> occursin(uuid, w), proxy.wants)

                # sandbox-facing kwargs carry the proxy coordinates, no mounts
                kwargs = PEF.seal_protocol_kwargs(ns)
                @test kwargs.env["PKGEVAL_CACHE_SERVER"] == base
                @test kwargs.env["PKGEVAL_CACHE_NAMESPACE"] == ns
                @test !haskey(kwargs, :mounts)
                # consumers never pin codegen; publishers pin ISA and image threads
                @test !haskey(kwargs.env, "JULIA_CPU_TARGET")
                @test !haskey(kwargs.env, "JULIA_IMAGE_THREADS")
                pub_kwargs = PEF.seal_protocol_kwargs(ns; publisher=true)
                @test pub_kwargs.env["JULIA_CPU_TARGET"] == PEF.seal_cpu_target()
                @test pub_kwargs.env["JULIA_IMAGE_THREADS"] == "1"

                # --- stage 2: wants become derivation jobs -------------------
                frame_rt = PEF.unframe_pair(PEF.frame_pair(Vector{UInt8}(b"JI"), Vector{UInt8}(b"SO")))
                @test frame_rt !== nothing && String(frame_rt[1]) == "JI" &&
                      String(frame_rt[2]) == "SO"
                @test PEF.unframe_pair(UInt8[1, 2, 3]) === nothing

                dep_uuid = "aaaaaaaa-0000-0000-0000-000000000002"
                dep_key, unit_key = "cd" ^ 32, "ef" ^ 32
                preimage = join(["v2", "julia=1.99.0+abc", "name=JSON",
                                 "uuid=$uuid", "version=1.0.0", "tree=$("11"^20)",
                                 "flags=163", "prefs=0",
                                 "dep=$dep_uuid:1f2e:4.1.0:$dep_key"], "\n")
                w = PEF.parse_want_preimage(preimage)
                @test w.name == "JSON" && w.version == "1.0.0"
                @test only(w.deps).key == dep_key && only(w.deps).version == "4.1.0"
                @test PEF.parse_want_preimage("v1\nuuid=x") === nothing
                @test PEF.parse_want_preimage("v2\nname=JSON") === nothing

                # publish a two-level closure with metadata: JSON@key -> Crayons@key
                mktempdir() do export_dir
                    mkpath(joinpath(export_dir, "compiled", "v1.13", "Crayons"))
                    write(joinpath(export_dir, "compiled", "v1.13", "Crayons", "Crayons_kk.ji"), "DEPJI")
                    open(joinpath(export_dir, "seal_keys.toml"), "w") do io
                        println(io, """
                            [Crayons]
                            uuid = "$dep_uuid"
                            key = "$dep_key"
                            version = "4.1.0"
                            preimage = "v2"
                            ji = "v1.13/Crayons/Crayons_kk.ji"
                            so = ""
                            deps = []
                            """)
                    end
                    @test PEF.publish_protocol!(sctx, ns, export_dir, "Crayons", dep_uuid)
                end
                mktempdir() do export_dir
                    mkpath(joinpath(export_dir, "compiled", "v1.13", "JSON"))
                    write(joinpath(export_dir, "compiled", "v1.13", "JSON", "JSON_kk.ji"), "UNITJI")
                    open(joinpath(export_dir, "seal_keys.toml"), "w") do io
                        println(io, """
                            [JSON]
                            uuid = "$uuid"
                            key = "$unit_key"
                            version = "1.0.0"
                            preimage = "v2"
                            ji = "v1.13/JSON/JSON_kk.ji"
                            so = ""

                            [[JSON.deps]]
                            uuid = "$dep_uuid"
                            name = "Crayons"
                            version = "4.1.0"
                            key = "$dep_key"
                            """)
                    end
                    @test PEF.publish_protocol!(sctx, ns, export_dir, "JSON", uuid)
                end

                # the closure walk follows the meta chain and pins everything
                pins, missing_keys = PEF.derivation_pins(sctx, ns, [(uuid, unit_key)])
                @test isempty(missing_keys)
                @test pins[dep_uuid]["version"] == "4.1.0"
                @test pins[uuid]["name"] == "JSON"
                # a root that was never published is reported missing
                _, missing2 = PEF.derivation_pins(sctx, ns, [(uuid, "12"^32)])
                @test missing2 == ["12"^32]

                # want ingestion: dedup'd derivation job + one seal-queue message
                @test PEF.ingest_want(sctx, ns, preimage) !== nothing
                @test PEF.ingest_want(sctx, ns, preimage) === nothing   # duplicate
                want_key = bytes2hex(PEF.SHA.sha256(codeunits(preimage)))
                c = PEF.claim_job(sctx; wait=1)
                @test c isa PEF.ClaimedJob
                @test PEF.is_derivation_job(c.job)
                @test c.job.package == want_key
                item = PEF.get_seal_item(sctx, c.job)
                @test item["name"] == "JSON" && item["uuid"] == uuid
                @test PEF.parse_want_preimage(item["preimage"]).version == "1.0.0"
                PEF.record_result(sctx, c, PEF.JobResult(; status="unsealable",
                                                         reason="missing_dependency",
                                                         duration=0.1))
                @test PEF.claim_job(sctx; wait=1) === nothing   # no duplicate message
                run = PEF.get_run(sctx, PEF.deriv_run_id(ns))
                @test run["kind"] == "deriv" && run["total_jobs"] == 1

                # the proxy route drives the same path end to end
                resp = ProxyHTTP.post("$base/want/v2/otherns"; body=preimage,
                                      status_exception=false)
                @test resp.status == 202
                c2 = PEF.claim_job(sctx; wait=1)
                @test c2 isa PEF.ClaimedJob && c2.job.run_id == "deriv-otherns"
                PEF.record_result(sctx, c2, PEF.JobResult(; status="unsealable",
                                                          reason="missing_dependency",
                                                          duration=0.1))

                # a dead derivation is not a tombstone: the same want re-arms
                # it (missing rungs may have landed since) and re-enqueues
                @test PEF.ingest_want(sctx, "otherns", preimage) !== nothing
                c3 = PEF.claim_job(sctx; wait=1)
                @test c3 isa PEF.ClaimedJob && c3.job.run_id == "deriv-otherns"
                item3 = PEF.get_seal_item(sctx, c3.job)
                @test item3["status"] == "running" && item3["blocked"] == 0

                # in-flight derivations make the proxy HOLD a matching GET
                # until the artifact lands (dataflow ordering by blocking);
                # a donor is summoned for the held slot's capacity
                donations = Ref(0)
                PEF.SEAL_DONOR[] = () -> (donations[] += 1)
                want_uuid = "aaaaaaaa-0000-0000-0000-000000000003"
                held_key = c3.job.package     # its item is `running` right now
                ProxyHTTP.get("$base/cache/v1/otherns/$want_uuid/$("66"^32)";
                              status_exception=false)   # warm the connection
                publisher = @async begin
                    sleep(6)
                    PEF.put_sealed_object(sctx,
                        PEF.kv_object_key("otherns", want_uuid, held_key),
                        Vector{UInt8}(b"HELD-FRAME"))
                end
                t0 = time()
                resp = ProxyHTTP.get("$base/cache/v1/otherns/$want_uuid/$held_key";
                                     status_exception=false)
                wait(publisher)
                @test resp.status == 200
                @test String(resp.body) == "HELD-FRAME"
                @test time() - t0 >= 4.0          # it actually held
                @test donations[] >= 1
                PEF.SEAL_DONOR[] = nothing
                PEF.record_result(sctx, c3, PEF.JobResult(; status="sealed", duration=0.1))

                # no derivation in flight for a key -> 404 without holding
                resp = ProxyHTTP.get("$base/cache/v1/otherns/$want_uuid/$("77"^32)";
                                     status_exception=false)
                @test resp.status == 404

                # --- /ensure: one preimage-carrying request ------------------
                # already-published context: served immediately
                pre_pub = join(["v2", "julia=1+a", "name=JSON", "uuid=$want_uuid",
                                "version=1.1.0", "tree=$("22"^20)", "flags=1", "prefs=0"], "
")
                key_pub = bytes2hex(PEF.SHA.sha256(codeunits(pre_pub)))
                PEF.put_sealed_object(sctx, PEF.kv_object_key("otherns", want_uuid, key_pub),
                                      Vector{UInt8}(b"ENSURED"))
                resp = ProxyHTTP.post("$base/ensure/v2/otherns"; body=pre_pub,
                                      status_exception=false)
                @test resp.status == 200
                @test String(resp.body) == "ENSURED"

                # novel context: the request *creates* the derivation and holds
                # on it; terminal failure releases as 404 (local compile is
                # then correct). The first requester no longer races its own
                # want.
                pre_new = join(["v2", "julia=1+a", "name=JSON", "uuid=$want_uuid",
                                "version=1.2.0", "tree=$("33"^20)", "flags=1", "prefs=0"], "
")
                key_new = bytes2hex(PEF.SHA.sha256(codeunits(pre_new)))
                failer = @async begin
                    sleep(3)
                    cd_ = PEF.claim_job(sctx; wait=2)
                    cd_ isa PEF.ClaimedJob || return
                    PEF.record_result(sctx, cd_, PEF.JobResult(; status="unsealable",
                                                               reason="precompile",
                                                               duration=0.1))
                end
                t0 = time()
                resp = ProxyHTTP.post("$base/ensure/v2/otherns"; body=pre_new,
                                      status_exception=false)
                wait(failer)
                @test resp.status == 404
                @test time() - t0 >= 2.0     # it held until the terminal flip
                item_new = PEF.get_seal_item(sctx, PEF.JobRef("deriv-otherns", "deriv", key_new))
                @test item_new !== nothing   # the ensure created the derivation
                @test item_new["status"] == "unsealable"

                # negative-cache race: the artifact publishes and the item goes
                # terminal while the key sits in the 30s negative cache (poisoned
                # by the pre-publication miss above) — a fresh request must still
                # serve it via the hold path's terminal store look, not 404
                PEF.put_sealed_object(sctx, PEF.kv_object_key("otherns", want_uuid, key_new),
                                      PEF.frame_pair(Vector{UInt8}(codeunits("RACEJI")), nothing))
                Dynamodb.update_item(
                    PEF.ddb_item(Dict("run_id" => "deriv-otherns",
                                      "job_key" => PEF.job_key("deriv", key_new))),
                    "pkgeval-jobs",
                    Dict("UpdateExpression" => "SET #s = :sealed",
                         "ExpressionAttributeNames" => Dict("#s" => "status"),
                         "ExpressionAttributeValues" => PEF.ddb_item(Dict(":sealed" => "sealed")));
                    aws_config=aws)
                resp = ProxyHTTP.post("$base/ensure/v2/otherns"; body=pre_new,
                                      status_exception=false)
                @test resp.status == 200
                @test occursin("RACEJI", String(resp.body))

                # nohold probe (derivations): published contexts serve even
                # while the key sits in the negative cache (poisoned by the
                # pre-publication miss above); novel contexts 404 immediately
                # without creating a derivation
                resp = ProxyHTTP.post("$base/ensure/v2/otherns"; body=pre_new,
                                      headers=["X-Nohold" => "1"],
                                      status_exception=false)
                @test resp.status == 200
                @test occursin("RACEJI", String(resp.body))
                pre_probe = join(["v2", "julia=1+a", "name=JSON", "uuid=$want_uuid",
                                  "version=1.3.0", "tree=$("44"^20)", "flags=1", "prefs=0"], "
")
                key_probe = bytes2hex(PEF.SHA.sha256(codeunits(pre_probe)))
                t0 = time()
                resp = ProxyHTTP.post("$base/ensure/v2/otherns"; body=pre_probe,
                                      headers=["X-Nohold" => "1"],
                                      status_exception=false)
                @test resp.status == 404
                @test time() - t0 < 2.0      # no hold
                @test PEF.get_seal_item(sctx, PEF.JobRef("deriv-otherns", "deriv",
                                                         key_probe)) === nothing

                # underivable want: a keyable dep without a published artifact
                # embeds a sandbox-local build_id no derivation can reproduce —
                # answered without holding and without creating a derivation
                dep_uuid_u = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
                dep_key_u = "ab"^32
                pre_und = join(["v2", "julia=1+a", "name=JSON", "uuid=$want_uuid",
                                "version=1.4.0", "tree=$("55"^20)", "flags=1", "prefs=0",
                                "dep=$dep_uuid_u:abc123:1.0.0:$dep_key_u"], "
")
                key_und = bytes2hex(PEF.SHA.sha256(codeunits(pre_und)))
                t0 = time()
                resp = ProxyHTTP.post("$base/ensure/v2/otherns"; body=pre_und,
                                      status_exception=false)
                @test resp.status == 404
                @test time() - t0 < 2.0      # no hold
                @test PEF.get_seal_item(sctx, PEF.JobRef("deriv-otherns", "deriv",
                                                         key_und)) === nothing
                # once the dep publishes, the same want is derivable: the
                # request now creates the derivation and holds on it
                PEF.put_sealed_object(sctx, PEF.kv_object_key("otherns", dep_uuid_u, dep_key_u),
                                      PEF.frame_pair(Vector{UInt8}(codeunits("DEPJI")), nothing))
                failer2 = @async begin
                    sleep(3)
                    cd_ = PEF.claim_job(sctx; wait=2)
                    cd_ isa PEF.ClaimedJob || return
                    PEF.record_result(sctx, cd_, PEF.JobResult(; status="unsealable",
                                                               reason="precompile",
                                                               duration=0.1))
                end
                t0 = time()
                resp = ProxyHTTP.post("$base/ensure/v2/otherns"; body=pre_und,
                                      status_exception=false)
                wait(failer2)
                @test resp.status == 404
                @test time() - t0 >= 2.0     # it held until the terminal flip
                @test PEF.get_seal_item(sctx, PEF.JobRef("deriv-otherns", "deriv",
                                                         key_und)) !== nothing

                # malformed preimages are rejected outright
                @test ProxyHTTP.post("$base/ensure/v2/otherns"; body="v1\ngarbage",
                                     status_exception=false).status == 400

                # --- crash-tolerant re-arm (the gh-5241627143 zombie class) --
                # a pending derivation whose message was lost (the ingester
                # died between item creation and send): a fresh want re-sends
                # instead of treating "pending" as already handled
                pre_z = join(["v2", "julia=1+a", "name=JSON", "uuid=$want_uuid",
                              "version=1.4.0", "tree=$("55"^20)", "flags=1", "prefs=0"], "\n")
                key_z = bytes2hex(PEF.SHA.sha256(codeunits(pre_z)))
                Dynamodb.put_item(PEF.ddb_item(Dict(
                        "run_id" => "deriv-otherns", "job_key" => PEF.job_key("deriv", key_z),
                        "config" => "deriv", "package" => key_z, "kind" => "deriv",
                        "name" => "JSON", "uuid" => want_uuid, "version" => "1.4.0",
                        "preimage" => pre_z, "status" => "pending", "attempts" => 0,
                        "blocked" => 0, "created_at" => PEF.isodate())),
                    "pkgeval-jobs"; aws_config=aws)      # no enqueued_at, no message
                @test PEF.claim_job(sctx; wait=1) === nothing        # truly no message
                @test PEF.ingest_want(sctx, "otherns", pre_z) == key_z   # re-armed
                cz = PEF.claim_job(sctx; wait=1)
                @test cz isa PEF.ClaimedJob && cz.job.package == key_z
                # a freshly-claimed derivation is alive: the same want must
                # neither re-send nor disturb it
                @test PEF.ingest_want(sctx, "otherns", pre_z) === nothing
                @test PEF.claim_job(sctx; wait=1) === nothing

                # a running derivation whose worker AND message died (spot
                # churn plus DLQ): started_at past any heartbeat lifetime
                # makes it re-armable
                Dynamodb.update_item(
                    PEF.ddb_item(Dict("run_id" => "deriv-otherns",
                                      "job_key" => PEF.job_key("deriv", key_z))),
                    "pkgeval-jobs",
                    Dict("UpdateExpression" => "SET started_at = :old REMOVE enqueued_at, heartbeat_at",
                         "ExpressionAttributeValues" => PEF.ddb_item(Dict(
                             ":old" => PEF.isodate(Dates.now(Dates.UTC) - Dates.Hour(5)))));
                    aws_config=aws)
                @test PEF.ingest_want(sctx, "otherns", pre_z) == key_z
                cz2 = PEF.claim_job(sctx; wait=1)
                @test cz2 isa PEF.ClaimedJob && cz2.job.package == key_z

                # holds are capped inside PkgEval's inactivity window, and the
                # holder heals what it waits on: holding on a zombie times out
                # (the requester then compiles locally) but leaves the
                # derivation re-armed with a live message
                Dynamodb.update_item(
                    PEF.ddb_item(Dict("run_id" => "deriv-otherns",
                                      "job_key" => PEF.job_key("deriv", key_z))),
                    "pkgeval-jobs",
                    Dict("UpdateExpression" => "SET #s = :pending REMOVE enqueued_at",
                         "ExpressionAttributeNames" => Dict("#s" => "status"),
                         "ExpressionAttributeValues" => PEF.ddb_item(Dict(":pending" => "pending")));
                    aws_config=aws)
                withenv("PKGEVAL_CACHE_HOLD_LIMIT" => "5") do
                    t0 = time()
                    @test PEF.hold_for_derivation(sctx, "otherns", want_uuid, key_z) === nothing
                    @test 4.0 <= time() - t0 < 30.0
                end
                ch = PEF.claim_job(sctx; wait=1)     # the holder re-sent the message
                @test ch isa PEF.ClaimedJob && ch.job.package == key_z
                PEF.record_result(sctx, ch, PEF.JobResult(; status="unsealable",
                                                          reason="precompile",
                                                          duration=0.1))

                # a deterministic conditional failure must fail fast, not burn
                # the retry backoff (five delays would take several seconds)
                t0 = time()
                @test_throws AWS.AWSException PEF.aws_retry() do
                    Dynamodb.put_item(PEF.ddb_item(Dict(
                            "run_id" => "deriv-otherns",
                            "job_key" => PEF.job_key("deriv", key_z))),
                        "pkgeval-jobs",
                        Dict("ConditionExpression" => "attribute_not_exists(run_id)");
                        aws_config=aws)
                end
                @test time() - t0 < 3.0
            finally
                PEF.stop_seal_proxy!()
            end
        end
    end

    @testset "broker STS against moto" begin
        with_env(Dict("AWS_ACCESS_KEY_ID" => "testing", "AWS_SECRET_ACCESS_KEY" => "testing",
                      "FARM_REGION" => "us-east-1", "STS_ENDPOINT" => endpoint)) do
            creds = FarmBroker.assume_role("arn:aws:iam::123456789012:role/pkgeval-worker",
                                           "keno"; duration=3600)
            @test !isempty(creds.access_key_id)
            @test !isempty(creds.session_token)
            # expiration parses and is in the future
            exp = PEF.parse_expiration(creds.expiration)
            @test exp > Dates.now(UTC)
        end
    end
finally
    kill(proc)
end

end # moto_available

end # module
