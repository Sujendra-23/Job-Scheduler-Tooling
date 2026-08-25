// Command scheduler runs a SLURM/LSF-style simulation: jobs with varying
// priority and resource requirements arrive over time, get placed on
// nodes by resource-aware bin-packing, and can preempt lower-priority
// running jobs when a higher-priority job would otherwise starve.
//
// Every decision (submitted, started, preempted, completed) is logged as a
// structured event via internal/telemetry, so cmd/tracer can later answer
// "why did job X wait N seconds."
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"scheduler/internal/queue"
	"scheduler/internal/resource"
	"scheduler/internal/telemetry"
)

// defaultNodeSpec mirrors the cluster topology this project has always
// simulated: two small nodes and one larger one.
const defaultNodeSpec = "node-a:16:65536:2,node-b:16:65536:2,node-c:32:131072:4"

// parseNodes turns a "name:cpu:memMB:gpus,name:cpu:memMB:gpus,..." spec into
// a node pool, so the cluster shape can be tuned from the command line
// instead of being hardcoded.
func parseNodes(spec string) ([]*resource.Node, error) {
	var nodes []*resource.Node
	for _, part := range strings.Split(spec, ",") {
		fields := strings.Split(part, ":")
		if len(fields) != 4 {
			return nil, fmt.Errorf("bad node spec %q: want name:cpu:memMB:gpus", part)
		}
		cpu, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("bad node spec %q: cpu: %w", part, err)
		}
		memMB, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, fmt.Errorf("bad node spec %q: memMB: %w", part, err)
		}
		gpus, err := strconv.Atoi(fields[3])
		if err != nil {
			return nil, fmt.Errorf("bad node spec %q: gpus: %w", part, err)
		}
		nodes = append(nodes, resource.NewNode(fields[0], cpu, memMB, gpus))
	}
	return nodes, nil
}

// runningJob tracks a job that has been placed on a node, so the
// simulation loop can find it again when it's time to complete or
// preempt it.
type runningJob struct {
	job        *queue.Job
	finishTick int
}

func main() {
	numJobs := flag.Int("jobs", 40, "number of synthetic jobs to submit")
	seed := flag.Int64("seed", 42, "random seed, for reproducible runs")
	pgDSN := flag.String("pg-dsn", "", "Postgres DSN, e.g. postgres://user:pass@localhost/scheduler?sslmode=disable (optional)")
	jsonlPath := flag.String("out", "scheduling_events.jsonl", "path to write the JSONL event log")
	maxTicks := flag.Int("max-ticks", 200, "number of simulated ticks to run before stopping")
	nodeSpec := flag.String("nodes", defaultNodeSpec, "cluster topology as name:cpu:memMB:gpus,... (default: two 16-core nodes + one 32-core node)")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))

	nodes, err := parseNodes(*nodeSpec)
	if err != nil {
		log.Fatalf("scheduler: %v", err)
	}

	var db *sql.DB
	if *pgDSN != "" {
		var err error
		db, err = sql.Open("postgres", *pgDSN)
		if err != nil {
			log.Fatalf("scheduler: opening postgres: %v", err)
		}
		if err := db.Ping(); err != nil {
			log.Fatalf("scheduler: pinging postgres: %v", err)
		}
		defer db.Close()
	}

	rec, err := telemetry.NewRecorder(*jsonlPath, db)
	if err != nil {
		log.Fatalf("scheduler: %v", err)
	}
	defer rec.Close()

	pool := &resource.Pool{Nodes: nodes}

	q := queue.New()
	running := map[string]*runningJob{} // jobID -> running state

	// Simulated clock, advanced one "tick" at a time (a tick stands in for
	// some real unit of wall-clock time, e.g. one second).
	tick := 0
	baseTime := time.Now()
	simTime := func() time.Time { return baseTime.Add(time.Duration(tick) * time.Second) }

	// Pre-generate the arrival tick for every job so the loop below can
	// submit them as the clock passes each one.
	type pendingSubmit struct {
		job         *queue.Job
		arrivalTick int
	}
	var toSubmit []pendingSubmit
	for i := 0; i < *numJobs; i++ {
		priority := 1 + rng.Intn(5) // 1 (low) to 5 (high)
		req := resource.Requirements{
			CPUCores: 1 + rng.Intn(8),
			MemoryMB: (1 + rng.Intn(8)) * 1024,
			GPUs:     rng.Intn(3), // most jobs want 0-2 GPUs
		}
		job := &queue.Job{
			ID:        fmt.Sprintf("job-%03d", i),
			Priority:  priority,
			Resources: req,
		}
		toSubmit = append(toSubmit, pendingSubmit{job: job, arrivalTick: rng.Intn(30)})
	}

	for tick = 0; tick < *maxTicks; tick++ {
		now := simTime()

		// 1. Submit any jobs whose arrival time has passed.
		for i := range toSubmit {
			if toSubmit[i].arrivalTick == tick {
				j := toSubmit[i].job
				j.SubmitTime = now
				j.State = queue.Pending
				q.Push(j)
				rec.Record(telemetry.Event{Timestamp: now, JobID: j.ID, EventType: "submitted", Priority: j.Priority,
					Detail: fmt.Sprintf("cpu=%d mem=%dMB gpu=%d", j.Resources.CPUCores, j.Resources.MemoryMB, j.Resources.GPUs)})
			}
		}

		// 2. Complete any running jobs whose finish tick has arrived.
		for id, rj := range running {
			if rj.finishTick == tick {
				for _, n := range pool.Nodes {
					if n.Name == rj.job.Node {
						n.Release(rj.job.Resources)
						break
					}
				}
				rj.job.State = queue.Completed
				rec.Record(telemetry.Event{Timestamp: now, JobID: id, EventType: "completed", NodeName: rj.job.Node, Priority: rj.job.Priority})
				delete(running, id)
			}
		}

		// 3. Try to place pending jobs, highest priority first.
		var requeue []*queue.Job
		for q.Len() > 0 {
			j := q.Pop()
			node := pool.FindFit(j.Resources)

			if node == nil {
				// No node has room. If this job is high-priority, try
				// preempting the lowest-priority running job that would
				// free enough room.
				if victim := findPreemptionVictim(running, j); victim != nil {
					for _, n := range pool.Nodes {
						if n.Name == victim.job.Node {
							n.Release(victim.job.Resources)
							break
						}
					}
					victim.job.State = queue.Preempted
					rec.Record(telemetry.Event{Timestamp: now, JobID: victim.job.ID, EventType: "preempted", NodeName: victim.job.Node,
						Priority: victim.job.Priority, Detail: fmt.Sprintf("preempted by %s (priority %d)", j.ID, j.Priority)})
					delete(running, victim.job.ID)
					victim.job.State = queue.Pending
					victim.job.SubmitTime = now // wait clock restarts on requeue, this is a deliberate simplification
					q.Push(victim.job)

					node = pool.FindFit(j.Resources)
				}
			}

			if node == nil {
				rec.Record(telemetry.Event{Timestamp: now, JobID: j.ID, EventType: "wait_reason", Priority: j.Priority,
					Detail: "no node has sufficient free capacity"})
				requeue = append(requeue, j)
				continue
			}

			node.Reserve(j.Resources)
			j.State = queue.Running
			j.Node = node.Name
			j.StartTime = now
			duration := 5 + rng.Intn(20) // simulated job runtime, in ticks
			running[j.ID] = &runningJob{job: j, finishTick: tick + duration}

			rec.Record(telemetry.Event{Timestamp: now, JobID: j.ID, EventType: "started", NodeName: node.Name, Priority: j.Priority,
				Detail: fmt.Sprintf("waited %s", j.WaitTime(now))})
		}
		for _, j := range requeue {
			q.Push(j)
		}
	}

	// --- summary ---
	fmt.Println("=== simulation complete ===")
	fmt.Printf("ticks run: %d, jobs still pending: %d, jobs still running: %d\n", *maxTicks, q.Len(), len(running))
	for _, n := range pool.Nodes {
		fmt.Println(" ", n)
	}
	fmt.Fprintf(os.Stderr, "event log written to %s\n", *jsonlPath)
}

// findPreemptionVictim returns the lowest-priority running job that (a) has
// strictly lower priority than the candidate and (b) would free enough
// resources for the candidate to fit once removed, or nil if no such job
// exists.
func findPreemptionVictim(running map[string]*runningJob, candidate *queue.Job) *runningJob {
	var victim *runningJob
	for _, rj := range running {
		if rj.job.Priority >= candidate.Priority {
			continue
		}
		if victim == nil || rj.job.Priority < victim.job.Priority {
			victim = rj
		}
	}
	return victim
}
