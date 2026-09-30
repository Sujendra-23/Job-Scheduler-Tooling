// Package dispatch owns the shared priority queue used by all worker
// processes. Workers claim and report jobs over net/rpc; the queue itself
// lives only in the coordinator, so a job can be claimed by at most one
// worker at a time.
package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"scheduler/internal/inference"
	"scheduler/internal/queue"
	"scheduler/internal/telemetry"
	"scheduler/internal/triage"
)

type ClaimArgs struct {
	WorkerID string
}

type ClaimReply struct {
	Job      *queue.Job
	Wait     bool
	Shutdown bool
}

type ReportArgs struct {
	WorkerID string
	JobID    string
	Success  bool
	Error    string
	Result   inference.Result
}

type ReportReply struct {
	Decision triage.Decision
}

type Stats struct {
	Submitted int
	Succeeded int
	Escalated int
	Retried   int
	Adjusted  int
}

type Broker struct {
	mu       sync.Mutex
	queue    *queue.Queue
	running  map[string]*queue.Job
	agent    triage.Agent
	recorder *telemetry.Recorder
	stats    Stats
	closed   bool
	done     chan struct{}
	doneOnce sync.Once
}

func NewBroker(agent triage.Agent, recorder *telemetry.Recorder) *Broker {
	return &Broker{
		queue: queue.New(), running: make(map[string]*queue.Job), agent: agent,
		recorder: recorder, done: make(chan struct{}),
	}
}

func (b *Broker) Submit(job *queue.Job) error {
	if err := job.Inference.Validate(); err != nil {
		return fmt.Errorf("job %s: %w", job.ID, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("submissions are closed")
	}
	if job.MaxAttempts < 1 {
		job.MaxAttempts = 3
	}
	job.State = queue.Pending
	if job.SubmitTime.IsZero() {
		job.SubmitTime = time.Now()
	}
	b.queue.Push(job)
	b.stats.Submitted++
	b.record(telemetry.Event{
		Timestamp: job.SubmitTime, JobID: job.ID, EventType: "submitted", Priority: job.Priority,
		Model: job.Inference.Model, BatchSize: job.Inference.BatchSize, Attempt: job.Attempt,
		Detail: fmt.Sprintf("inference batch model=%s shape=%dx%dx%dx%d precision=%s",
			job.Inference.Model, job.Inference.BatchSize, job.Inference.Channels,
			job.Inference.Height, job.Inference.Width, job.Inference.Precision),
	})
	return nil
}

func (b *Broker) CloseSubmissions() {
	b.mu.Lock()
	b.closed = true
	b.maybeDoneLocked()
	b.mu.Unlock()
}

// Claim is an RPC method. It atomically removes the highest-priority job
// from the shared queue and assigns it to one worker process.
func (b *Broker) Claim(args ClaimArgs, reply *ClaimReply) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.queue.Len() == 0 {
		if b.closed && len(b.running) == 0 {
			reply.Shutdown = true
			b.maybeDoneLocked()
		} else {
			reply.Wait = true
		}
		return nil
	}
	job := b.queue.Pop()
	job.Attempt++
	job.State = queue.Running
	job.StartTime = time.Now()
	job.Node = args.WorkerID
	b.running[job.ID] = job
	reply.Job = job
	b.record(telemetry.Event{
		Timestamp: job.StartTime, JobID: job.ID, EventType: "started", NodeName: args.WorkerID,
		Priority: job.Priority, Model: job.Inference.Model, BatchSize: job.Inference.BatchSize, Attempt: job.Attempt,
		Detail: fmt.Sprintf("worker=%s waited=%s", args.WorkerID, job.WaitTime(job.StartTime)),
	})
	return nil
}

// Report is an RPC method. Failed jobs are removed from the worker and sent
// through triage before being requeued, adjusted, or terminally escalated.
func (b *Broker) Report(args ReportArgs, reply *ReportReply) error {
	b.mu.Lock()
	job, ok := b.running[args.JobID]
	if !ok {
		b.mu.Unlock()
		return fmt.Errorf("job %s is not assigned", args.JobID)
	}
	delete(b.running, args.JobID)
	if args.Success {
		job.State = queue.Completed
		b.stats.Succeeded++
		resultJSON, _ := json.Marshal(args.Result)
		b.record(telemetry.Event{
			Timestamp: time.Now(), JobID: job.ID, EventType: "completed", NodeName: args.WorkerID,
			Priority: job.Priority, Model: job.Inference.Model, BatchSize: job.Inference.BatchSize, Attempt: job.Attempt,
			Detail: string(resultJSON),
		})
		b.maybeDoneLocked()
		b.mu.Unlock()
		return nil
	}
	b.record(telemetry.Event{
		Timestamp: time.Now(), JobID: job.ID, EventType: "failed", NodeName: args.WorkerID,
		Priority: job.Priority, Model: job.Inference.Model, BatchSize: job.Inference.BatchSize,
		Attempt: job.Attempt, Detail: args.Error,
	})
	b.mu.Unlock()

	failure := triage.Failure{
		JobID: job.ID, Attempt: job.Attempt, MaxAttempts: job.MaxAttempts,
		Error: args.Error, Batch: job.Inference,
	}
	decision, err := b.agent.Decide(context.Background(), failure)
	if err != nil {
		decision = triage.Decision{Action: triage.Escalate, Reason: "triage agent failed: " + err.Error(), Source: "safety_fallback"}
	}
	if err := decision.Validate(); err != nil {
		decision = triage.Decision{Action: triage.Escalate, Reason: "invalid triage decision: " + err.Error(), Source: "safety_fallback"}
	}
	if job.Attempt >= job.MaxAttempts && decision.Action != triage.Escalate {
		decision = triage.Decision{Action: triage.Escalate, Reason: "retry budget exhausted", Source: "coordinator_guardrail"}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	reply.Decision = decision
	b.record(telemetry.Event{
		Timestamp: time.Now(), JobID: job.ID, EventType: "triaged", NodeName: args.WorkerID,
		Priority: job.Priority, Model: job.Inference.Model, BatchSize: job.Inference.BatchSize,
		Attempt: job.Attempt, TriageAction: decision.Action,
		Detail: fmt.Sprintf("source=%s reason=%s", decision.Source, decision.Reason),
	})

	switch decision.Action {
	case triage.Retry:
		job.State = queue.Pending
		job.InjectFailure = ""
		b.queue.Push(job)
		b.stats.Retried++
		b.recordLifecycle(job, "retried", decision.Reason)
	case triage.AutoAdjust:
		job.State = queue.Pending
		job.InjectFailure = ""
		job.Inference.BatchSize = decision.BatchSize
		job.Inference.Precision = decision.Precision
		b.queue.Push(job)
		b.stats.Adjusted++
		b.recordLifecycle(job, "adjusted", decision.Reason)
	case triage.Escalate:
		job.State = queue.Completed
		b.stats.Escalated++
		b.recordLifecycle(job, "escalated", decision.Reason)
	}
	b.maybeDoneLocked()
	return nil
}

func (b *Broker) recordLifecycle(job *queue.Job, eventType, detail string) {
	b.record(telemetry.Event{
		Timestamp: time.Now(), JobID: job.ID, EventType: eventType, NodeName: job.Node,
		Priority: job.Priority, Model: job.Inference.Model, BatchSize: job.Inference.BatchSize,
		Attempt: job.Attempt, Detail: detail,
	})
}

func (b *Broker) record(event telemetry.Event) {
	if b.recorder != nil {
		b.recorder.Record(event)
	}
}

func (b *Broker) maybeDoneLocked() {
	if b.closed && b.queue.Len() == 0 && len(b.running) == 0 && b.stats.Succeeded+b.stats.Escalated == b.stats.Submitted {
		b.doneOnce.Do(func() { close(b.done) })
	}
}

func (b *Broker) Done() <-chan struct{} { return b.done }

func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}
