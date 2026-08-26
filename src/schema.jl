# Data model shared by submitters, workers, the broker and the report generator.
#
# DynamoDB is the source of truth:
#   runs table:  pk `run_id`   — one item per submitted evaluation run
#   jobs table:  pk `run_id`, sk `job_key` ("<config>#<package>") — one item per job
# SQS only dispatches job references; S3 holds logs and reports.

export FarmConfig, FarmCtx

"Names/locations of the AWS resources making up a farm deployment."
Base.@kwdef struct FarmConfig
    region::String
    queue_url::String
    runs_table::String
    jobs_table::String
    bucket::String
    # queue for jobs with long estimated durations (see expand_run's dynamic
    # cutoff); workers poll it first. Empty = single-queue mode: SQS standard
    # queues are nowhere near FIFO under backlog (observed to be close to
    # recency-biased), so priority *between* queues is the only ordering tool.
    slow_queue_url::String = ""
    # queue for compilecache seal jobs (docs/sealing.md); workers poll it before
    # anything else. Empty = sealing disabled: the farm behaves exactly as before
    seal_queue_url::String = ""
    # queue for derivation jobs, claimed with priority above even seal jobs:
    # derivations fetch with nohold probes, so they are the always-runnable
    # leaves of the sealing dataflow — sharing the seal queue inverted the
    # dependency order (seal jobs held on derivations buried behind them) and
    # head-of-line blocking idled the fleet. Empty = derivations ride the seal
    # queue, exactly the pre-split behavior.
    deriv_queue_url::String = ""
    # name of the build-request Lambda, invoked via the plain Invoke API;
    # empty when the deployment has no build-request broker
    build_request_function::String = ""
end

"The slow-jobs queue, falling back to the main queue when none is configured."
slow_queue(cfg::FarmConfig) =
    isempty(cfg.slow_queue_url) ? cfg.queue_url : cfg.slow_queue_url

"The derivations queue, falling back to the seal queue when none is configured."
deriv_queue(cfg::FarmConfig) =
    isempty(cfg.deriv_queue_url) ? cfg.seal_queue_url : cfg.deriv_queue_url

FarmConfig(d::AbstractDict) = FarmConfig(; region=d["region"], queue_url=d["queue_url"],
                                         runs_table=d["runs_table"], jobs_table=d["jobs_table"],
                                         bucket=d["bucket"],
                                         slow_queue_url=get(d, "slow_queue_url", ""),
                                         seal_queue_url=get(d, "seal_queue_url", ""),
                                         deriv_queue_url=get(d, "deriv_queue_url", ""),
                                         build_request_function=get(d, "build_request_function", ""))

function farm_config_from_env(env=ENV)
    FarmConfig(; region=env["AWS_REGION"], queue_url=env["PKGEVAL_QUEUE_URL"],
               runs_table=env["PKGEVAL_RUNS_TABLE"], jobs_table=env["PKGEVAL_JOBS_TABLE"],
               bucket=env["PKGEVAL_BUCKET"],
               slow_queue_url=get(env, "PKGEVAL_SLOW_QUEUE_URL", ""),
               seal_queue_url=get(env, "PKGEVAL_SEAL_QUEUE_URL", ""),
               deriv_queue_url=get(env, "PKGEVAL_DERIV_QUEUE_URL", ""),
               build_request_function=get(env, "PKGEVAL_BUILD_REQUEST_FUNCTION", ""))
end

Base.Dict(cfg::FarmConfig) =
    Dict("region" => cfg.region, "queue_url" => cfg.queue_url, "runs_table" => cfg.runs_table,
         "jobs_table" => cfg.jobs_table, "bucket" => cfg.bucket,
         "slow_queue_url" => cfg.slow_queue_url,
         "seal_queue_url" => cfg.seal_queue_url,
         "deriv_queue_url" => cfg.deriv_queue_url,
         "build_request_function" => cfg.build_request_function)

"A FarmConfig plus the AWS credentials/config used to talk to it."
struct FarmCtx
    cfg::FarmConfig
    aws::AWS.AbstractAWSConfig
end


## identifiers and S3 layout

new_run_id(now::DateTime=Dates.now(UTC)) =
    Dates.format(now, "yyyymmdd-HHMMSS") * "-" * Random.randstring(RandomDevice(), "0123456789abcdef", 6)

job_key(config::AbstractString, package::AbstractString) = string(config, "#", package)
split_job_key(key::AbstractString) = Tuple(split(key, "#"; limit=2))

log_key(run_id, config, package, attempt::Integer=1) =
    "runs/$run_id/logs/$config/$package" * (attempt == 1 ? "" : ".$attempt") * ".log"
report_key(run_id, name) = "runs/$run_id/report/$name"

"""
Content fingerprint of a configuration dict, ignoring its display `name`:
two configs with equal fingerprints request the same evaluation, which is what
makes results transferable between runs (see `expand_run`'s baseline reuse).
"""
config_fingerprint(d::AbstractDict) =
    bytes2hex(SHA.sha256(join(("$k=$(JSON.json(d[k]))" for k in sort!([String(k) for k in keys(d)])
                               if k != "name"), "\n")))

"""
Whether a julia spec names an immutable build — an exact commit or a release
tag. Only those are sound to reuse results for: a moving ref like `#master` or
`nightly` names different Julias at different times, so runs against it only
ever match if the submitter pinned the sha (the bot always does).
"""
reusable_julia_spec(spec::AbstractString) =
    occursin(r"^[^#]+#[0-9a-f]{40}$", spec) || occursin(r"^v\d+\.\d+\.\d+$", spec)


## DynamoDB attribute-value marshalling
#
# We only need the S/N/BOOL/NULL/L/M subset. Numbers travel as strings per the
# DynamoDB wire format; `ddb_unwrap` brings them back as Int/Float64.

ddb_wrap(v::AbstractString) = Dict("S" => String(v))
ddb_wrap(v::Bool) = Dict("BOOL" => v)
ddb_wrap(v::Real) = Dict("N" => string(v))
ddb_wrap(::Nothing) = Dict("NULL" => true)
ddb_wrap(v::AbstractVector) = Dict("L" => [ddb_wrap(x) for x in v])
ddb_wrap(v::AbstractDict) = Dict("M" => Dict(String(k) => ddb_wrap(x) for (k, x) in v))

ddb_item(d::AbstractDict) = Dict(String(k) => ddb_wrap(v) for (k, v) in d)

function ddb_unwrap(av::AbstractDict)
    type, v = only(av)
    if type == "S"
        v
    elseif type == "N"
        parsed = tryparse(Int, v)
        parsed !== nothing ? parsed : parse(Float64, v)
    elseif type == "BOOL"
        v
    elseif type == "NULL"
        nothing
    elseif type == "L"
        Any[ddb_unwrap(x) for x in v]
    elseif type == "M"
        Dict{String,Any}(k => ddb_unwrap(x) for (k, x) in v)
    else
        error("unsupported DynamoDB attribute type: $type")
    end
end

ddb_parse(item::AbstractDict) = Dict{String,Any}(k => ddb_unwrap(v) for (k, v) in item)


## Configuration <-> JSON-able dict
#
# Only the *modified* settings are serialized (plus the name), so runs stay valid
# even if PkgEval's defaults evolve. Workers overwrite `cpus` locally anyway.

# the enum type itself is not exported from PkgEval, but its values are
const RRMode = typeof(PkgEval.RREnabled)

function config_to_dict(cfg::PkgEval.Configuration)
    d = Dict{String,Any}("name" => cfg.name)
    for field in fieldnames(PkgEval.Configuration)
        field === :name && continue
        PkgEval.ismodified(cfg, field) || continue
        val = getproperty(cfg, field)
        d[String(field)] = val isa Symbol ? String(val) :
                           val isa RRMode ? String(Symbol(val)) :
                           val
    end
    return d
end

# convert a JSON-decoded value back to the type of `Setting{T}` field `field`
function convert_setting(::Type{T}, val) where {T}
    if T === Symbol
        Symbol(val)
    elseif T === RRMode
        val isa AbstractString ? getproperty(PkgEval, Symbol(val))::RRMode : RRMode(val)
    elseif T <: AbstractVector
        convert(T, [convert_setting(eltype(T), x) for x in val])
    else
        convert(T, val)
    end
end

function config_from_dict(d::AbstractDict)
    kwargs = Dict{Symbol,Any}(:name => d["name"])
    for (k, v) in d
        k == "name" && continue
        field = Symbol(k)
        hasfield(PkgEval.Configuration, field) ||
            error("run was submitted with a configuration setting `$k` that this " *
                  "worker's PkgEval does not understand; upgrade the worker")
        T = fieldtype(PkgEval.Configuration, field)  # Setting{X}
        kwargs[field] = convert_setting(T.parameters[1], v)
    end
    return PkgEval.Configuration(; kwargs...)
end


## records

"Everything needed to describe a run; stored on the runs item."
struct RunSpec
    configs::Vector{PkgEval.Configuration}
    packages::Vector{String}
    # free-form submission context, e.g. GitHub coordinates the bot should report to
    context::Dict{String,Any}
end

struct JobRef
    run_id::String
    config::String
    package::String
end

job_key(job::JobRef) = job_key(job.config, job.package)

JobRef(d::AbstractDict) = JobRef(d["run_id"], d["config"], d["package"])
json_message(job::JobRef) =
    JSON.json(Dict("run_id" => job.run_id, "config" => job.config, "package" => job.package))

"Final result of one job, as recorded in DynamoDB (log goes to S3)."
Base.@kwdef struct JobResult
    status::String          # PkgEval status: test/load (= success) | fail | crash | kill
                            # | skip, or "error" for infrastructure failures
    reason::Union{String,Nothing} = nothing
    version::Union{String,Nothing} = nothing
    duration::Float64 = 0.0
    # wall time the slot spent on the whole evaluation (setup, install,
    # precompile, test), which is what cost metering must price — `duration` is
    # only the test/load phase's CPU time and misses ~60% of slot occupancy
    # (skips and kills bill zero by it despite minutes of real slot time)
    wall::Float64 = 0.0
    # CPU slots the job occupied (1 except for WIDE_PACKAGES); cost metering
    # prices wall × slots
    slots::Int = 1
    # peak cgroup memory of the sandbox in bytes (page cache included), for
    # memory-aware scheduling backtests; nothing when the metric was unavailable
    peak_rss::Union{Int,Nothing} = nothing
    log::Union{String,Nothing} = nothing  # uploaded to S3, not stored in DynamoDB
end

const TERMINAL_STATUSES = ("test", "load", "fail", "crash", "kill", "skip", "error",
                           # seal jobs only (docs/sealing.md); never appear in user runs
                           "sealed", "unsealable")
issuccess(status::AbstractString) = status in ("test", "load")

reason_message(reason::Nothing) = ""
reason_message(reason::AbstractString) =
    reason == "worker_exception" ? "the worker failed to evaluate the package" :
    PkgEval.reason_message(Symbol(reason))

# error_line heuristics, mirroring the log-tail highlighting in site/index.html
# (analyzeTail): crash-class markers trump the first ERROR: line, which in turn
# trumps a plain test failure location
const ERROR_LINE_PRIORITY = (r"Assertion .* failed", r"Internal error:",
                             r"Unreachable reached", r"GC error",
                             r"ERROR: Method overwriting is not permitted")
# ERROR: lines that only restate that testing failed, never why
const ERROR_LINE_GENERIC = ("Some tests did not pass", "errored during testing",
                            "/proc/self/stat", "failed process", "Test run finished",
                            "ProcessExited")
const ANSI_OR_CR = r"\e\[[0-9;]*[A-Za-z]|\r"

"""
    error_line(log) -> Union{String,Nothing}

The first meaningful error line of a failing job's log — stored per job so reports
can cluster shared failure signatures without refetching logs. Kept short: it is
free-form text on a DynamoDB item that every full `run_jobs` read pays for.
"""
function error_line(log::AbstractString)
    lines = split(replace(log, ANSI_OR_CR => ""), '\n')
    for line in lines, pattern in ERROR_LINE_PRIORITY
        occursin(pattern, line) && return error_line_clip(line)
    end
    for line in lines
        l = lstrip(line)
        if (startswith(l, "ERROR: ") || startswith(l, "UNHANDLED TASK ERROR: ")) &&
           !any(g -> occursin(g, l), ERROR_LINE_GENERIC)
            return error_line_clip(line)
        end
    end
    for line in lines
        occursin("Test Failed at", line) && return error_line_clip(line)
    end
    return nothing
end

function error_line_clip(line::AbstractString)
    s = strip(line)
    length(s) <= 240 ? String(s) : first(s, 239) * "…"
end
