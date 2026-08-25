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
| `-jobs` | how many synthetic jobs to generate (default 40) |
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
| `internal/resource` | fit/reserve/release, best-fit slack selection, no-fit case |
| `internal/telemetry` | event recorder creates/writes the JSONL file, nil-DB safety, bad-path error |
| `cmd/tracer` | log parsing (incl. malformed lines), job timeline reporting, summary stats |
| `cmd/scheduler` | `-nodes` spec parsing (valid, default, malformed) |

`cmd/scheduler`'s `main` itself (flag wiring, DB connection) and
`mem/allocator.c` are exercised by running them (see **Running it** above
and `make allocator`), not by unit tests.

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
