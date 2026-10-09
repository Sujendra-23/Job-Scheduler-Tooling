# Distributed Inference Job Scheduler & Debugging Tooling

A local distributed job-dispatch system for prioritized ML inference batches,
with multiple OS worker processes, reproducible failure injection, agentic
failure triage, structured telemetry, and a tracing CLI. The repository also
retains its original SLURM/LSF-style resource-placement simulation and
free-list memory allocator study.

## Requirements

- Go 1.22+
- Python 3.10+
- PyTorch + torchvision (optional, for real ResNet-50; see `requirements-ml.txt`)
- `gcc` (only for `make allocator`)
- PostgreSQL (optional, only for `make run-sim-pg`)

## Quick start with PyTorch

Keep the ML dependencies isolated in a project-local virtual environment:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-ml.txt

make build
make run-distributed
```

You can verify that the real torchvision backend is being used by inspecting a
completion event:

```bash
tail -n 1 scheduling_events.jsonl
```

Its `detail` field will contain `"backend":"torchvision-resnet50"`. If the
virtual environment is not activated or PyTorch cannot be imported,
`-backend auto` uses the clearly labeled `cpu-reference-mlp` backend instead.

## Layout

```
cmd/scheduler/       simulation: job arrivals, placement, preemption, completion
cmd/dispatcher/      coordinator + separately spawned inference worker process
cmd/tracer/           debugging CLI: per-job timeline, wait-time stats, device config status
cmd/devices/          device registry simulation: versioned config push with ack/retry + HTTP status
cmd/technicians/      technician dispatch simulation: skills + travel time, reassignment, dead-letter
internal/devices/     registry, simulated lossy network + device workers
internal/technician/  skill + travel-time dispatcher, OSRM/haversine routing clients
ml/                   persistent ResNet-50 / CPU-reference inference runner
internal/dispatch/    synchronized priority broker and job lifecycle; SQS-backed coordinator + worker loop
internal/sqsqueue/    SQS queue-per-priority-tier + DLQ wrapper (LocalStack-tested)
internal/inference/   typed ResNet-50 batch and result contracts
internal/resource/    node capacity model + best-fit placement
internal/queue/        priority heap with FIFO-within-tier ordering
internal/telemetry/    structured event logging (JSONL + Postgres sink)
internal/triage/      LLM and deterministic fallback triage agents
mem/allocator.c        free-list allocator with fragmentation tracking
sql/schema.sql         Postgres schema + analytics queries (joins/aggregations)
Jenkinsfile            CI: build, unit tests, allocator regression gate, sim run
```

## Distributed inference path

`dispatcher` creates actual inference payloads using the same contract as the
sibling GPU Benchmarking Suite: `torchvision.models.resnet50`, NCHW
`[batch, 3, 224, 224]` inputs, and fp32/fp16 precision. Each long-lived worker
loads its model once, repeatedly pulls the highest-priority available batch
from the coordinator's shared queue, runs a forward pass, and reports latency
and top-1 class IDs.

Workers are separate OS processes communicating with the coordinator over TCP
RPC. This is a genuine process boundary and shared work queue, but it is
intentionally documented as a **single-host simulation of distribution**. The
RPC worker subcommand can also be launched on another host if the coordinator
listener is made externally reachable; authentication/TLS are not included.

When `torch`/`torchvision` are installed, `-backend auto` runs ResNet-50 on
CUDA when present or CPU otherwise. On a machine without those packages it
falls back to a dependency-free two-layer CPU reference model. That fallback
performs a real neural-network forward pass for CI, but is not represented as
ResNet-50 performance; every completion event records its actual backend.

### Run it

```bash
make run-distributed

# Deterministic CI/demo run; all first attempts fail, exercising every action.
./bin/dispatcher -jobs 6 -workers 3 -backend cpu-reference \
  -failure-rate 1 -seed 7 -out /tmp/distributed.jsonl

# Require the real torchvision ResNet-50 backend (no automatic fallback).
./bin/dispatcher -jobs 4 -workers 2 -python .venv/bin/python \
  -backend torch -failure-rate 0 -out /tmp/resnet50.jsonl
```

The second command produces OOM, transient reset, and corrupt-input failures.
The triage layer respectively auto-adjusts the batch, retries it, or escalates
it. Injection is consumed after the first attempt so retries test recovery
instead of creating an infinite artificial failure loop.

| Flag | Meaning |
|---|---|
| `-workers` | number of separately spawned worker processes |
| `-backend` | `auto`, `torch`, or dependency-free `cpu-reference` |
| `-failure-rate` | fraction of first attempts receiving deterministic failure injection |
| `-triage-mode` | `policy` (offline/reproducible) or `llm` |
| `-triage-url` | OpenAI-compatible chat-completions endpoint |
| `-triage-model` | model name served by that endpoint |
| `-triage-api-key` | API key; prefer `TRIAGE_LLM_API_KEY` instead |

### Agentic failure triage

LLM mode sends the failed batch shape, precision, attempt count, retry budget,
and error to a model. The model must choose exactly one tool-like action:
`retry`, `auto_adjust`, or `escalate`. Decisions are schema-validated, and the
coordinator independently prevents the model from exceeding the retry budget.
If the model endpoint is unavailable or returns invalid output, an explicit
`policy_fallback` decision is recorded rather than losing the job.

```bash
export TRIAGE_LLM_URL=http://localhost:8000/v1/chat/completions
export TRIAGE_LLM_MODEL=my-instruct-model
export TRIAGE_LLM_API_KEY=...

./bin/dispatcher -jobs 12 -workers 3 -triage-mode llm
```

Telemetry now covers `failed`, `triaged`, `retried`, `adjusted`, and
`escalated` in addition to the original lifecycle events. Each record includes
model, batch size, attempt, worker ID, and triage action. The Postgres schema
contains idempotent migration statements for these fields.

### Verified real-model run

The torchvision path has been executed end to end on Apple Silicon using
PyTorch 2.14.0 and torchvision 0.29.0. A batch-size-1 ResNet-50 job was claimed
by a worker process, completed successfully, and recorded:

```json
{"backend":"torchvision-resnet50","device":"cpu","batch_size":1,"precision":"fp32","latency_ms":18.2936,"top1_class_ids":[844]}
```

This is a measured local result, not a projected GPU benchmark. Hardware,
backend, model warm-up, and batch size materially affect latency.

## SQS-backed queue (optional, LocalStack-verified)

`dispatcher -queue-backend sqs` swaps the in-process priority heap for Amazon
SQS. The default (`-queue-backend broker`) is unchanged.

- **One queue per priority tier**: `<prefix>-p1` ... `<prefix>-p5`. Workers
  sweep tiers highest-first (`internal/sqsqueue`), so priority is strict across
  tiers and FIFO-ish within a tier (standard queues give best-effort ordering).
- **Visibility timeout** (`-sqs-visibility-timeout`, default 30s): a received
  batch is hidden from other workers; the worker acks (deletes) it only after
  the result is reported. If the worker dies mid-job the message simply
  reappears after the timeout and another worker takes it.
- **Dead-letter queue** `<prefix>-dlq` with `maxReceiveCount`
  (`-sqs-max-receive`, default 3) on every tier queue: a batch that keeps
  killing workers is moved to the DLQ instead of looping forever. The
  coordinator watches the DLQ and records a terminal `dead_lettered` event.
- Triage still applies: `retry` = release the message for immediate redelivery,
  `auto_adjust` = enqueue the adjusted batch and ack the old one, `escalate` =
  ack and record. Delivery is **at-least-once**, so completion is idempotent:
  a batch redelivered after it was already reported complete is acked and
  logged as `duplicate_delivery`, never double-counted.
- New telemetry events (visible with `tracer -job <id>`): `redelivered`
  (previous holder never acked), `dead_lettered`, `duplicate_delivery`.

```bash
docker run -d --rm --name localstack-sqs -p 4566:4566 -e SERVICES=sqs localstack/localstack:3.8
export SQS_ENDPOINT=http://localhost:4566
make build
./scripts/sqs-redelivery-demo.sh                  # kill-a-worker demo (below)
go test ./... -v                                  # SQS integration tests run when SQS_ENDPOINT is set, skip otherwise
./bin/dispatcher -queue-backend sqs -jobs 12 -workers 3 -backend cpu-reference
```

`-chaos-poison N` / `-chaos-hang N` make the dispatcher SIGKILL the worker
process that receives those jobs (and start a replacement), which is how the
demo proves recovery with real process deaths rather than a mocked failure.

Abbreviated output of `scripts/sqs-redelivery-demo.sh` (visibility timeout 4s,
max receive 3, 8 jobs, 3 workers; SIGKILLs are real `kill -9`s of worker
processes):

```
chaos: SIGKILL worker-01 while it held inference-000 (receive_count=1)
chaos: SIGKILL worker-06 while it held inference-000 (receive_count=2)
chaos: SIGKILL worker-04 while it held inference-000 (receive_count=3)
worker processes spawned=8 submitted=8 succeeded=7 escalated=0 dead_lettered=1 retried=0 adjusted=2 redelivered=4 duplicates=0
inference-000: started x3 (each holder killed) -> dead_lettered
inference-001: started (worker killed) -> redelivered after the visibility timeout -> started on another worker -> completed
```

Caveats: tested against **LocalStack 3.8 only**, not real AWS SQS (real SQS
short-polling can return empty on a non-empty queue and visibility expiry is
not instantaneous; the worker loop just re-polls). There is no visibility
heartbeat, so a job that legitimately runs longer than the visibility timeout
would be redelivered while still running (and then deduplicated by the
coordinator). Workers still call a single-host coordinator over RPC for
telemetry/triage; only the queue itself moved to SQS.

## Device registry with versioned config push (single-host simulation)

`internal/devices` + `cmd/devices` model the delivery logic of pushing
versioned configuration to a fleet of device workers. **This is a single-host
simulation**: the devices are in-process structs behind a lossy in-memory
"network" with seeded fault injection. There is no real device protocol, no
real network, and the registry state is in-memory (a registry restart resets
versions).

- Each device has its own monotonically increasing **config version**
  (`SetConfig` bumps it).
- A push waits up to `-ack-timeout` for an **ack**; on timeout it retries with
  exponential backoff, **bounded** by `-max-attempts`, after which the device is
  marked `failed` (an operator can `POST /devices/{id}/retry`).
- **Devices ignore stale or duplicate versions**: a push with version <= the one
  the device runs is not applied, but the device still acks the version it runs,
  so a lost ack is recovered by retrying and a re-delivery applies exactly once.
- The registry ignores acks whose version is not the desired one, and a newer
  `SetConfig` **supersedes** an older push still being retried.
- **Status** is visible via HTTP and via the tracing CLI (events are written as
  telemetry with job id `device:<id>`).

```bash
make build
./bin/devices -demo -devices 6 -drop-ack-rate 0.4 -drop-push-rate 0.2 -out devices.jsonl
./bin/tracer -log devices.jsonl -devices            # per-device desired/acked/state/attempts table
./bin/tracer -log devices.jsonl -job device:cam-01  # full push/timeout/ack timeline

./bin/devices -addr :8190 -devices 3 -drop-ack-rate 0.5 &   # serve mode
curl -X PUT -d '{"fps":"30"}' localhost:8190/devices/cam-01/config   # -> {"version":1}
curl localhost:8190/devices                                           # all statuses (JSON)
curl localhost:8190/devices/cam-01                                    # one device
```

Tests (`go test ./internal/devices/ ./cmd/tracer/`, also run with `-race`):
ack marks the version delivered; **retry after a dropped ack** (device applied
once, received twice); retry after dropped pushes; retry bounded then `failed`
(and recovers via `Retry`); **idempotent re-delivery** and stale-version
rejection on the device; a stale ack never completes the desired version; a
newer config supersedes an in-flight push; independent per-device versions;
the HTTP endpoints; and the tracer's per-device status table.

## Technician dispatch (single-process simulation)

`internal/technician` + `cmd/technicians` assign field work orders to technicians. It is a
separate entry point (like `cmd/devices`), so the ML inference dispatcher and the SQS path are
untouched.

* **Skill match is a hard filter**: a technician must have every skill the order needs.
* **Travel time picks among the qualified**: each candidate's ETA is "when they finish what is
  already planned, plus the drive to this job", so a nearby technician with a full route loses
  to a slightly farther idle one. Orders are inserted into a route by priority (FIFO within a
  tier); this is a greedy assignment, not a full vehicle-routing optimizer.
* **Routing** goes through a `Router` interface. `OSRMRouter` calls OSRM's Table service (the
  same OSRM the `ai_maps` project uses for route geometry); `HaversineRouter` is a no-network
  estimate (straight line x 1.3 at 40 km/h); `FallbackRouter` uses the second when the first
  errors, and every assignment records which one produced its ETA (`osrm`, `haversine`,
  `haversine-fallback`). Tests use fake routers and `httptest`, so they need no network.
* **Events that change the plan**: `Cancel` and `RunningLate` trigger a rebalance that
  reconsiders every planned (not started) job and moves it only if another technician arrives at
  least 5 minutes sooner (`Options.MinImprovement`), so near-ties do not ping-pong.
  `ReportFailure` retries on a different technician.
* **Idempotency and dead letters follow the SQS coordinator's rules**: completing, cancelling or
  failing a job that is already terminal (`completed`, `cancelled`, `dead_lettered`) is a counted
  no-op (`duplicate_completion` event), and an order is dead-lettered after `MaxAttempts`
  failures or after `MaxAttempts` passes with no qualified technician in the fleet. Waiting
  because qualified technicians are merely full does not count toward dead-lettering. These are
  in-memory states, not SQS messages; this mode does not use SQS.
* Events go to the same telemetry recorder, so `tracer -log technicians.jsonl -job WO-104` works.

```bash
make run-technicians                    # haversine estimates, scripted day with a cancel, a late
                                        # report, a failure, a dead letter and a duplicate completion
OSRM_URL=http://localhost:5000 make run-technicians   # OSRM travel times (haversine if it is down)
```

**What was and was not run.** All tests are offline (`go test ./...`, also `-race`).
OSRM was exercised by hand against the *public demo server* for one live table request and one
run of the scenario (one request timed out and correctly fell back to haversine); that is a smoke
test of the request/response shape, not a load or accuracy check, and the demo server is
rate-limited and not for production use. Nothing was run against a self-hosted OSRM. The
dispatcher holds its lock while calling the router (bounded by a 3 s HTTP timeout), state is
in memory and lost on restart, and there are no time windows, shift hours or real-time traffic.

## Original scheduler simulation

ResNet-50 inference batches arrive with a random priority (1-5), batch size,
precision, and derived CPU/memory/GPU requirements.
Each simulation tick: newly-arrived jobs are submitted, finished jobs
release their resources, then pending jobs are placed highest-priority
first using best-fit-by-CPU-slack. If a high-priority job can't be placed,
the scheduler looks for the lowest-priority *running* job with strictly
lower priority and preempts it, requeuing the preempted job. Every
decision - submitted, started, preempted, completed, and each tick a job
is told to keep waiting - is written as a structured event.

## Running it

```bash
make build
make run-distributed      # multi-process inference dispatch
make run-sim              # writes scheduling_events.jsonl, no DB needed
make trace                 # prints aggregate wait-time stats

# with Postgres (see sql/schema.sql to create the DB/tables first):
make run-sim-pg

make allocator             # builds and runs the fragmentation study
```

`cmd/scheduler` also takes `-max-ticks` (simulation length, default 200)
and `-nodes` (cluster topology as `name:cpu:memMB:gpus,...`, default
`node-a:16:65536:2,node-b:16:65536:2,node-c:32:131072:4`) if you want to
try a shorter run or a different cluster shape without editing the code.

## Giving it input and seeing the output

The two binaries are separate: `scheduler` generates and runs a simulated
workload (that's where you supply input), `tracer` reads the log
`scheduler` wrote and reports on it (that's where you see output).

**1. Run the scheduler with your own input:**

```bash
./bin/scheduler -jobs 40 -seed 1 -out /tmp/run.jsonl
```

| Flag | Meaning |
|---|---|
| `-jobs` | how many inference-batch jobs to generate (default 40) |
| `-seed` | random seed - same seed + same `-jobs` always reproduces the exact same run |
| `-out` | where to write the JSONL event log |
| `-max-ticks` | how many simulated ticks to run before stopping (default 200) |
| `-nodes` | cluster topology, `name:cpu:memMB:gpus,...` (default: two 16-core nodes + one 32-core node) |
| `-pg-dsn` | optional Postgres DSN - also writes every event to the `scheduling_events` table |

This prints a short on-screen summary (ticks run, jobs left pending/running,
final utilization per node) and writes the full event-by-event detail to
the file you named with `-out`.

**2. See the output with the tracer:**

```bash
# aggregate wait-time stats across every job in the run
./bin/tracer -log /tmp/run.jsonl -summary

# full timeline for one specific job
./bin/tracer -log /tmp/run.jsonl -job job-005
```

`-summary` prints min/median/max/mean wait time across all jobs.
`-job <id>` prints that job's full history - submitted, any times it was
told to keep waiting, preempted (if it happened), started, completed -
plus its total wait time.

**3. Or read the raw output yourself:**

```bash
cat /tmp/run.jsonl
```

Every line is one JSON event (`submitted`, `started`, `preempted`,
`completed`, or `wait_reason`) with a timestamp, job ID, priority, and
node name where relevant - `tracer` is just a formatter over this file.

## Sample results

A 40-job run against the default 3-node pool (16/16/32 CPU cores, 2/2/4
GPUs) over 200 simulated ticks completed all 40 jobs, with cluster
utilization returning to 0% at the end (no resource leaks).

Aggregate wait time by priority tier, from the join query in
`sql/schema.sql` against the Postgres sink:

| Priority | Avg wait | Max wait |
|---|---|---|
| 5 (highest) | 3.4s | 9s |
| 4 | 3.8s | 25s |
| 3 | 9.6s | 45s |
| 2 | 15.9s | 56s |
| 1 (lowest) | 28.1s | 58s |

Wait time decreases monotonically with priority, which is the expected
behavior of the placement/preemption logic in `cmd/scheduler`. Tracing a
single job (`tracer -job job-005`) shows the same story at the per-job
level - submitted, preempted twice by higher-priority jobs, then started:
```
submitted at 04:31:24, started at 04:31:33 -> waited 9s (told to wait 6 times)
```

**Memory allocator fragmentation study** - `mem/allocator.c` run against a
4MB heap with 20,000 alloc/free operations:

| Strategy | Final free blocks | Final external fragmentation | Failed allocations |
|---|---|---|---|
| First-fit | 1343 | 99.1% | 5046 |
| Best-fit | 1154 | 98.8% | 4998 |

Both strategies degrade badly under this workload, since small,
no-coalescing-opportunity allocations dominate a small heap. Best-fit
consistently produces fewer free blocks and fewer failed allocations than
first-fit, the expected direction of the effect.

## Tests

```bash
make test
# equivalent to:
go test ./... -v -cover
```

This runs every `_test.go` file in the module and prints each test name
plus a coverage percentage per package, e.g.:

```
=== RUN   TestPopReturnsHighestPriorityFirst
--- PASS: TestPopReturnsHighestPriorityFirst (0.00s)
...
ok  	scheduler/internal/queue	0.23s	coverage: 72.2% of statements
```

What's covered:

| Package | Tested |
|---|---|
| `internal/queue` | priority ordering, FIFO-within-tier, empty pop, peek |
| `internal/inference` | ResNet-50 batch construction and validation |
| `internal/dispatch` | cross-worker claims, priority ordering, adjustment/requeue, completion; SQS coordinator idempotency, killed-worker redelivery, poison-job DLQ (SQS tests need LocalStack) |
| `internal/sqsqueue` | tier priority order, visibility-timeout redelivery, DLQ after maxReceiveCount, immediate release (needs LocalStack) |
| `internal/triage` | retry/adjust/escalate policy, retry limits, mocked LLM response parsing |
| `internal/resource` | fit/reserve/release, best-fit slack selection, no-fit case |
| `internal/telemetry` | event recorder creates/writes the JSONL file, nil-DB safety, bad-path error |
| `internal/devices` | config push ack, retry on dropped ack/push, bounded retry -> failed, idempotent re-delivery, stale ack/version handling, supersede, HTTP API |
| `internal/technician` | skill filtering, ETA = queue + drive, priority insertion, capacity, cancel/running-late rebalancing with hysteresis, idempotent completion/cancel/failure, retry then dead-letter, no-qualified dead-letter, routing fallback, leg cache, OSRM request/response/errors (`httptest`), concurrency (`-race`); `TestOSRMLive` runs only if `OSRM_URL` is set |
| `cmd/tracer` | log parsing (incl. malformed lines), job timeline reporting, summary stats, device status table |
| `cmd/scheduler` | `-nodes` spec parsing (valid, default, malformed) |

`cmd/scheduler`'s `main` itself (flag wiring, DB connection) and
`mem/allocator.c` are exercised by running them (see **Running it** above
and `make allocator`), not by unit tests.

## Known limitations

- Distributed workers currently run on one host by default. TCP RPC creates
  real process/network boundaries, but the coordinator has no TLS, worker
  identity, replicated queue, or failover and is not production cluster
  infrastructure.
- The original `cmd/scheduler` resource/preemption study remains a
  single-threaded tick simulation. The new `cmd/dispatcher` is the executable
  that performs concurrent multi-process inference work.
- ResNet-50 uses `weights=None`, exactly like GPU Suite's benchmark harness,
  so it benchmarks/executes the model graph but its class IDs are not useful
  predictions until a trained checkpoint is supplied.
- The offline triage policy exists for reproducible local/CI runs. Use
  `-triage-mode llm` to exercise an actual language-model decision; its
  fallback source remains visible in telemetry.
- A preempted job's wait clock restarts on requeue
  (`cmd/scheduler/main.go`, where the job is pushed back onto the queue),
  a deliberate simplification - the priority-vs-wait numbers above measure
  wait since the most recent (re)submission, not total time since first
  arrival.
- The `Jenkinsfile` assumes a Jenkins agent with a Postgres service
  container available; its stages have been reasoned through but not run
  against a live Jenkins instance.
