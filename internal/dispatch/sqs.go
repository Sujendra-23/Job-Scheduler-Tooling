package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"scheduler/internal/inference"
	"scheduler/internal/queue"
	"scheduler/internal/sqsqueue"
	"scheduler/internal/telemetry"
	"scheduler/internal/triage"
)

// This file is the SQS-backed alternative to Broker. The queue (priority
// tiers, redelivery, dead-lettering) lives in SQS; the SQSCoordinator keeps
// only what SQS cannot: telemetry, failure triage, and an idempotent record of
// which jobs reached a terminal state. Workers pull from SQS directly and talk
// to the coordinator over RPC for Started/Report/Status.

// Chaos injection kinds (queue.Job.InjectFailure). They model a worker that
// stalls/dies while holding a batch, as opposed to the inference errors
// ("oom", "transient", "corrupt_input") that triage handles.
const (
	InjectHang   = "hang"   // first delivery only: holder never acks
	InjectPoison = "poison" // every delivery: holder never acks
)

type StartedArgs struct {
	WorkerID     string
	Job          queue.Job
	Attempt      int
	ReceiveCount int
	Tier         int
}

type StartedReply struct {
	// Duplicate means the job already reached a terminal state (e.g. it was
	// completed by a worker that died before deleting the message). The
	// worker must just ack the message and skip it.
	Duplicate bool
}

type SQSReportArgs struct {
	WorkerID string
	Job      queue.Job
	Attempt  int
	Success  bool
	Error    string
	Result   inference.Result
}

type SQSReportReply struct {
	Duplicate bool
	Decision  triage.Decision
}

type StatusArgs struct{}
type StatusReply struct{ Done bool }

type SQSStats struct {
	Submitted    int
	Succeeded    int
	Escalated    int
	DeadLettered int
	Retried      int
	Adjusted     int
	Redelivered  int // deliveries after a holder died/stalled (not triaged retries)
	Duplicates   int // deliveries/reports for jobs already terminal
}

// Control is the worker's view of the coordinator; satisfied directly by
// *SQSCoordinator and by an RPC client adapter.
type Control interface {
	Started(args StartedArgs, reply *StartedReply) error
	Report(args SQSReportArgs, reply *SQSReportReply) error
	Status(args StatusArgs, reply *StatusReply) error
}

type SQSCoordinator struct {
	mu       sync.Mutex
	agent    triage.Agent
	recorder *telemetry.Recorder
	qs       *sqsqueue.Queues

	jobs     map[string]*queue.Job // as submitted
	terminal map[string]string     // job -> completed | escalated | dead_lettered
	released map[string]bool       // jobs a worker was told to retry (planned redelivery)
	stats    SQSStats
	closed   bool
	done     chan struct{}
	doneOnce sync.Once

	// OnStarted, if set, is called (outside the lock) for each delivery.
	// The dispatcher uses it to SIGKILL the worker holding a chaos job.
	OnStarted func(args StartedArgs)
}

func NewSQSCoordinator(agent triage.Agent, recorder *telemetry.Recorder, qs *sqsqueue.Queues) *SQSCoordinator {
	return &SQSCoordinator{
		agent: agent, recorder: recorder, qs: qs,
		jobs: map[string]*queue.Job{}, terminal: map[string]string{}, released: map[string]bool{},
		done: make(chan struct{}),
	}
}

func (c *SQSCoordinator) Submit(ctx context.Context, job *queue.Job) error {
	var err error
	if job.Sandbox != nil {
		err = job.Sandbox.Validate()
	} else {
		err = job.Inference.Validate()
	}
	if err != nil {
		return fmt.Errorf("job %s: %w", job.ID, err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("submissions are closed")
	}
	if job.MaxAttempts < 1 {
		job.MaxAttempts = 3
	}
	job.State = queue.Pending
	if job.SubmitTime.IsZero() {
		job.SubmitTime = time.Now()
	}
	c.jobs[job.ID] = job
	c.stats.Submitted++
	c.record(job, "submitted", "", job.Attempt, fmt.Sprintf("sqs tier=p%d model=%s batch=%d precision=%s",
		sqsqueue.Tier(job.Priority), job.Inference.Model, job.Inference.BatchSize, job.Inference.Precision))
	c.mu.Unlock()
	if err := c.qs.Send(ctx, job); err != nil {
		return fmt.Errorf("enqueue %s: %w", job.ID, err)
	}
	return nil
}

func (c *SQSCoordinator) CloseSubmissions() {
	c.mu.Lock()
	c.closed = true
	c.maybeDoneLocked()
	c.mu.Unlock()
}

func (c *SQSCoordinator) Started(args StartedArgs, reply *StartedReply) error {
	c.mu.Lock()
	job := &args.Job
	if state, ok := c.terminal[job.ID]; ok {
		c.stats.Duplicates++
		c.record(job, "duplicate_delivery", args.WorkerID, args.Attempt,
			fmt.Sprintf("job already %s; acking and skipping (receive_count=%d)", state, args.ReceiveCount))
		reply.Duplicate = true
		c.mu.Unlock()
		return nil
	}
	if args.ReceiveCount > 1 && !c.released[job.ID] {
		c.stats.Redelivered++
		c.record(job, "redelivered", args.WorkerID, args.Attempt, fmt.Sprintf(
			"receive_count=%d: previous holder never acked within the visibility timeout (died or stalled)", args.ReceiveCount))
	}
	delete(c.released, job.ID)
	c.record(job, "started", args.WorkerID, args.Attempt,
		fmt.Sprintf("worker=%s tier=p%d receive_count=%d", args.WorkerID, args.Tier, args.ReceiveCount))
	hook := c.OnStarted
	c.mu.Unlock()
	if hook != nil {
		hook(args)
	}
	return nil
}

func (c *SQSCoordinator) Report(args SQSReportArgs, reply *SQSReportReply) error {
	job := &args.Job
	c.mu.Lock()
	if _, ok := c.terminal[job.ID]; ok {
		c.stats.Duplicates++
		reply.Duplicate = true
		c.mu.Unlock()
		return nil
	}
	if args.Success {
		c.terminal[job.ID] = "completed"
		c.stats.Succeeded++
		resultJSON, _ := json.Marshal(args.Result)
		c.record(job, "completed", args.WorkerID, args.Attempt, string(resultJSON))
		c.maybeDoneLocked()
		c.mu.Unlock()
		return nil
	}
	c.record(job, "failed", args.WorkerID, args.Attempt, args.Error)
	c.mu.Unlock()

	decision, err := c.agent.Decide(context.Background(), triage.Failure{
		JobID: job.ID, Attempt: args.Attempt, MaxAttempts: job.MaxAttempts, Error: args.Error, Batch: job.Inference,
	})
	if err != nil {
		decision = triage.Decision{Action: triage.Escalate, Reason: "triage agent failed: " + err.Error(), Source: "safety_fallback"}
	}
	if err := decision.Validate(); err != nil {
		decision = triage.Decision{Action: triage.Escalate, Reason: "invalid triage decision: " + err.Error(), Source: "safety_fallback"}
	}
	if args.Attempt >= job.MaxAttempts && decision.Action != triage.Escalate {
		decision = triage.Decision{Action: triage.Escalate, Reason: "retry budget exhausted", Source: "coordinator_guardrail"}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.terminal[job.ID]; ok { // finished elsewhere while we were triaging
		c.stats.Duplicates++
		reply.Duplicate = true
		return nil
	}
	reply.Decision = decision
	c.recordTriage(job, args, decision)
	switch decision.Action {
	case triage.Retry:
		c.released[job.ID] = true
		c.stats.Retried++
		c.record(job, "retried", args.WorkerID, args.Attempt, decision.Reason)
	case triage.AutoAdjust:
		c.stats.Adjusted++
		c.record(job, "adjusted", args.WorkerID, args.Attempt, decision.Reason)
	case triage.Escalate:
		c.terminal[job.ID] = "escalated"
		c.stats.Escalated++
		c.record(job, "escalated", args.WorkerID, args.Attempt, decision.Reason)
	}
	c.maybeDoneLocked()
	return nil
}

func (c *SQSCoordinator) Status(_ StatusArgs, reply *StatusReply) error {
	select {
	case <-c.done:
		reply.Done = true
	default:
	}
	return nil
}

// WatchDeadLetters polls the DLQ and turns each new entry into a terminal
// "dead_lettered" event. DLQ messages are left in place for inspection.
func (c *SQSCoordinator) WatchDeadLetters(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-t.C:
		}
		dead, err := c.qs.PeekDeadLetters(ctx)
		if err != nil {
			continue
		}
		c.mu.Lock()
		for _, d := range dead {
			if _, seen := c.terminal[d.Job.ID]; seen {
				continue
			}
			c.terminal[d.Job.ID] = "dead_lettered"
			c.stats.DeadLettered++
			detail := fmt.Sprintf("exceeded maxReceiveCount=%d without an ack; moved to DLQ (message %s)", c.qs.Config().MaxReceiveCount, d.MessageID)
			if d.Class != "" { // sent to the DLQ explicitly, e.g. by the sandbox
				detail = fmt.Sprintf("%s: %s (message %s)", d.Class, d.Detail, d.MessageID)
			}
			c.record(d.Job, "dead_lettered", "", d.Job.Attempt, detail)
		}
		c.maybeDoneLocked()
		c.mu.Unlock()
	}
}

func (c *SQSCoordinator) Done() <-chan struct{} { return c.done }

func (c *SQSCoordinator) Stats() SQSStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *SQSCoordinator) maybeDoneLocked() {
	if c.closed && len(c.terminal) >= c.stats.Submitted {
		c.doneOnce.Do(func() { close(c.done) })
	}
}

func (c *SQSCoordinator) recordTriage(job *queue.Job, args SQSReportArgs, d triage.Decision) {
	if c.recorder == nil {
		return
	}
	c.recorder.Record(telemetry.Event{
		Timestamp: time.Now(), JobID: job.ID, EventType: "triaged", NodeName: args.WorkerID,
		Priority: job.Priority, Model: job.Inference.Model, BatchSize: job.Inference.BatchSize,
		Attempt: args.Attempt, TriageAction: d.Action,
		Detail: fmt.Sprintf("source=%s reason=%s", d.Source, d.Reason),
	})
}

func (c *SQSCoordinator) record(job *queue.Job, eventType, worker string, attempt int, detail string) {
	if c.recorder == nil {
		return
	}
	c.recorder.Record(telemetry.Event{
		Timestamp: time.Now(), JobID: job.ID, EventType: eventType, NodeName: worker,
		Priority: job.Priority, Model: job.Inference.Model, BatchSize: job.Inference.BatchSize,
		Attempt: attempt, Detail: detail,
	})
}

// Runner executes one batch. attempt is 1-based across redeliveries/retries.
type Runner func(job *queue.Job, attempt int) (inference.Result, error)

// RunSQSWorker is the worker loop: receive (highest tier first), report to
// the coordinator, run, report the outcome, then ack. The ack (SQS delete) is
// always last, so a worker that dies anywhere before it leaves the message to
// be redelivered after the visibility timeout. It returns nil when the
// coordinator reports everything terminal, or ctx.Err() on cancellation.
func RunSQSWorker(ctx context.Context, qs *sqsqueue.Queues, ctl Control, workerID string, run Runner) error {
	return RunSQSWorkerWith(ctx, qs, ctl, workerID, run, WorkerOptions{})
}

// RunSQSWorkerWith is RunSQSWorker with optional capabilities (see
// WorkerOptions). Jobs that carry a Sandbox spec never reach run: they go to
// the sandbox runner, and failures to the dead-letter queue.
func RunSQSWorkerWith(ctx context.Context, qs *sqsqueue.Queues, ctl Control, workerID string, run Runner, opts WorkerOptions) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		d, err := qs.Receive(ctx)
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}
		if d == nil {
			var st StatusReply
			if err := ctl.Status(StatusArgs{}, &st); err != nil {
				return fmt.Errorf("status: %w", err)
			}
			if st.Done {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		job := d.Job
		attempt := job.Attempt + d.ReceiveCount // Attempt carries tries made before an auto-adjust re-enqueue
		var sr StartedReply
		if err := ctl.Started(StartedArgs{WorkerID: workerID, Job: *job, Attempt: attempt, ReceiveCount: d.ReceiveCount, Tier: d.Tier}, &sr); err != nil {
			return fmt.Errorf("started: %w", err)
		}
		if sr.Duplicate {
			if err := qs.Ack(ctx, d); err != nil {
				return err
			}
			continue
		}

		if job.InjectFailure == InjectPoison || (job.InjectFailure == InjectHang && attempt == 1) {
			// Simulated stall: hold the batch and never ack. Whoever supervises
			// this process is expected to kill it; SQS then redelivers.
			<-ctx.Done()
			return ctx.Err()
		}

		if job.Sandbox != nil {
			if err := runSandboxedJob(ctx, qs, ctl, opts, workerID, d, attempt); err != nil {
				return err
			}
			continue
		}

		result, runErr := run(job, attempt)
		report := SQSReportArgs{WorkerID: workerID, Job: *job, Attempt: attempt, Success: runErr == nil, Result: result}
		if runErr != nil {
			report.Error = runErr.Error()
		}
		var rr SQSReportReply
		if err := ctl.Report(report, &rr); err != nil {
			return fmt.Errorf("report %s: %w", job.ID, err)
		}
		if runErr == nil || rr.Duplicate {
			if err := qs.Ack(ctx, d); err != nil {
				return err
			}
			continue
		}
		switch rr.Decision.Action {
		case triage.Retry:
			err = qs.Release(ctx, d)
		case triage.AutoAdjust:
			adjusted := *job
			adjusted.Inference.BatchSize = rr.Decision.BatchSize
			adjusted.Inference.Precision = rr.Decision.Precision
			adjusted.Attempt = attempt
			adjusted.InjectFailure = ""
			if err = qs.Send(ctx, &adjusted); err == nil {
				err = qs.Ack(ctx, d)
			}
		default: // escalate: terminal, drop the message
			err = qs.Ack(ctx, d)
		}
		if err != nil {
			return fmt.Errorf("apply triage decision for %s: %w", job.ID, err)
		}
	}
}
