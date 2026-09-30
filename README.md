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

## Layout

```
cmd/scheduler/       simulation: job arrivals, placement, preemption, completion
cmd/dispatcher/      coordinator + separately spawned inference worker process
cmd/tracer/           debugging CLI: per-job timeline + aggregate wait-time stats
ml/                   persistent ResNet-50 / CPU-reference inference runner
internal/dispatch/    synchronized priority broker and job lifecycle
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
| `internal/dispatch` | cross-worker claims, priority ordering, adjustment/requeue, completion |
| `internal/triage` | retry/adjust/escalate policy, retry limits, mocked LLM response parsing |
| `internal/resource` | fit/reserve/release, best-fit slack selection, no-fit case |
| `internal/telemetry` | event recorder creates/writes the JSONL file, nil-DB safety, bad-path error |
| `cmd/tracer` | log parsing (incl. malformed lines), job timeline reporting, summary stats |
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
