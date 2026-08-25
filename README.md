# System-Level Job Scheduler & Debugging Tooling

A SLURM/LSF-inspired job scheduler with resource-aware placement and
priority preemption, a free-list memory allocator simulator for studying
fragmentation, and a tracing CLI that answers "why did job X wait" from
structured event telemetry logged to both a JSONL file and PostgreSQL.

## Requirements

- Go 1.22+
- `gcc` (only for `make allocator`)
- PostgreSQL (optional, only for `make run-sim-pg`)

## Layout

```
cmd/scheduler/       simulation: job arrivals, placement, preemption, completion
cmd/tracer/           debugging CLI: per-job timeline + aggregate wait-time stats
internal/resource/    node capacity model + best-fit placement
internal/queue/        priority heap with FIFO-within-tier ordering
internal/telemetry/    structured event logging (JSONL + Postgres sink)
mem/allocator.c        free-list allocator with fragmentation tracking
sql/schema.sql         Postgres schema + analytics queries (joins/aggregations)
Jenkinsfile            CI: build, unit tests, allocator regression gate, sim run
```

## How the scheduler works

Jobs arrive with a random priority (1-5) and CPU/memory/GPU requirements.
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
go test ./... -v -cover
```

`internal/queue`, `internal/resource`, and `internal/telemetry` have unit
tests; `cmd/tracer`'s parsing/reporting logic (`loadEvents`, `traceJob`,
`summary`) and `cmd/scheduler`'s node-spec parsing are also covered.
`cmd/scheduler`'s `main` itself (flag wiring, DB connection) and
`mem/allocator.c` are exercised by running them, not by unit tests.

## Known limitations

- The cluster topology and simulation length are now flags (`-nodes`,
  `-max-ticks`), but the simulation loop itself is single-threaded and
  tick-based - it models the scheduling *algorithm*, not the concurrency
  or networking a real scheduler daemon would need.
- A preempted job's wait clock restarts on requeue
  (`cmd/scheduler/main.go`, where the job is pushed back onto the queue),
  a deliberate simplification - the priority-vs-wait numbers above measure
  wait since the most recent (re)submission, not total time since first
  arrival.
- The `Jenkinsfile` assumes a Jenkins agent with a Postgres service
  container available; its stages have been reasoned through but not run
  against a live Jenkins instance.
