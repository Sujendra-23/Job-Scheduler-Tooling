package dispatch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"scheduler/internal/inference"
	"scheduler/internal/queue"
	"scheduler/internal/sqsqueue"
	"scheduler/internal/triage"
)

// ---- coordinator logic: no SQS needed ----

func started(c *SQSCoordinator, j *queue.Job, receive int) StartedReply {
	var r StartedReply
	c.Started(StartedArgs{WorkerID: "w", Job: *j, Attempt: j.Attempt + receive, ReceiveCount: receive, Tier: 3}, &r)
	return r
}

func TestSQSCoordinatorCompletionIsIdempotent(t *testing.T) {
	c := NewSQSCoordinator(triage.PolicyAgent{}, nil, nil)
	j := testJob(t, "a", 3, 8)
	c.stats.Submitted, c.closed = 1, true
	started(c, j, 1)
	var r1, r2 SQSReportReply
	c.Report(SQSReportArgs{WorkerID: "w", Job: *j, Attempt: 1, Success: true}, &r1)
	// The worker died after reporting but before deleting the message, so the
	// batch is redelivered and re-reported: it must not be double counted.
	if !started(c, j, 2).Duplicate {
		t.Fatal("redelivery of a completed job must be flagged duplicate")
	}
	c.Report(SQSReportArgs{WorkerID: "w2", Job: *j, Attempt: 2, Success: true}, &r2)
	if !r2.Duplicate || c.Stats().Succeeded != 1 || c.Stats().Duplicates != 2 {
		t.Fatalf("stats = %+v dup=%v", c.Stats(), r2.Duplicate)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("coordinator should be done")
	}
}

func TestSQSCoordinatorDistinguishesDeadHolderFromPlannedRetry(t *testing.T) {
	c := NewSQSCoordinator(triage.PolicyAgent{}, nil, nil)
	j := testJob(t, "b", 3, 8)
	c.stats.Submitted = 1
	started(c, j, 1)
	var rep SQSReportReply
	c.Report(SQSReportArgs{WorkerID: "w", Job: *j, Attempt: 1, Error: "transient GPU worker reset"}, &rep)
	if rep.Decision.Action != triage.Retry {
		t.Fatalf("decision = %+v", rep.Decision)
	}
	started(c, j, 2) // planned retry: ReceiveCount 2 but NOT a dead holder
	if c.Stats().Redelivered != 0 {
		t.Fatalf("planned retry counted as redelivery: %+v", c.Stats())
	}
	started(c, j, 3) // no triage release before this one: previous holder died
	if c.Stats().Redelivered != 1 {
		t.Fatalf("dead-holder redelivery not counted: %+v", c.Stats())
	}
}

func TestSQSCoordinatorRetryBudgetEscalates(t *testing.T) {
	c := NewSQSCoordinator(triage.PolicyAgent{}, nil, nil)
	j := testJob(t, "c", 3, 8)
	c.stats.Submitted, c.closed = 1, true
	var rep SQSReportReply
	c.Report(SQSReportArgs{WorkerID: "w", Job: *j, Attempt: 3, Error: "transient GPU worker reset"}, &rep)
	if rep.Decision.Action != triage.Escalate || c.Stats().Escalated != 1 {
		t.Fatalf("decision=%+v stats=%+v", rep.Decision, c.Stats())
	}
}

// ---- integration: coordinator + worker loops over LocalStack ----

func sqsTestQueues(t *testing.T, vis time.Duration, maxReceive int) *sqsqueue.Queues {
	t.Helper()
	ep := os.Getenv("SQS_ENDPOINT")
	if ep == "" {
		t.Skip("SQS_ENDPOINT not set; skipping SQS integration test")
	}
	qs, err := sqsqueue.Create(context.Background(), sqsqueue.Config{
		Endpoint: ep, Prefix: fmt.Sprintf("d%d", time.Now().UnixNano()), VisibilityTimeout: vis, MaxReceiveCount: maxReceive,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { qs.Delete(context.Background()) })
	return qs
}

func okRunner(job *queue.Job, attempt int) (inference.Result, error) {
	return inference.Result{Backend: "test", BatchSize: job.Inference.BatchSize}, nil
}

// A worker takes a batch and "dies" (its context is cancelled while it holds
// the job, so it never acks). A second worker must receive the same job after
// the visibility timeout and finish it.
func TestSQSKilledWorkerBatchIsRedeliveredAndCompleted(t *testing.T) {
	qs := sqsTestQueues(t, 2*time.Second, 3)
	c := NewSQSCoordinator(triage.PolicyAgent{}, nil, qs)
	j := testJob(t, "victim", 4, 8)
	j.InjectFailure = InjectHang
	if err := c.Submit(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	c.CloseSubmissions()

	ctx1, kill := context.WithCancel(context.Background())
	held := make(chan struct{})
	c.OnStarted = func(a StartedArgs) {
		if a.WorkerID == "worker-1" {
			close(held)
		}
	}
	w1done := make(chan error, 1)
	go func() { w1done <- RunSQSWorker(ctx1, qs, c, "worker-1", okRunner) }()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("worker-1 never received the job")
	}
	kill() // worker dies mid-job
	<-w1done

	w2ctx, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	if err := RunSQSWorker(w2ctx, qs, c, "worker-2", okRunner); err != nil {
		t.Fatalf("worker-2: %v", err)
	}
	s := c.Stats()
	if s.Succeeded != 1 || s.Redelivered != 1 {
		t.Fatalf("want 1 success after 1 redelivery, got %+v", s)
	}
}

// A job that kills every worker that touches it ("poison") is delivered
// exactly MaxReceiveCount times and then lands in the DLQ; the coordinator
// observes it and finishes instead of waiting forever.
func TestSQSPoisonJobEndsInDeadLetterQueue(t *testing.T) {
	const maxReceive = 2
	qs := sqsTestQueues(t, 1*time.Second, maxReceive)
	c := NewSQSCoordinator(triage.PolicyAgent{}, nil, qs)
	j := testJob(t, "poison", 4, 8)
	j.InjectFailure = InjectPoison
	c.Submit(context.Background(), j)
	c.CloseSubmissions()

	ctx, stop := context.WithTimeout(context.Background(), 40*time.Second)
	defer stop()
	go c.WatchDeadLetters(ctx, 300*time.Millisecond)

	var mu sync.Mutex
	deliveries := 0
	c.OnStarted = func(StartedArgs) { mu.Lock(); deliveries++; mu.Unlock() }
	for i := 0; i < 8 && c.Stats().DeadLettered == 0; i++ {
		// each "worker" grabs the poison batch, then is killed
		wctx, kill := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { RunSQSWorker(wctx, qs, c, fmt.Sprintf("w%d", i), okRunner); close(done) }()
		time.Sleep(2500 * time.Millisecond)
		kill()
		<-done
	}
	select {
	case <-c.Done():
	case <-time.After(15 * time.Second):
		t.Fatalf("coordinator never observed the DLQ entry; stats=%+v", c.Stats())
	}
	mu.Lock()
	defer mu.Unlock()
	if c.Stats().DeadLettered != 1 || deliveries != maxReceive {
		t.Fatalf("want dead-lettered after exactly %d deliveries, got deliveries=%d stats=%+v", maxReceive, deliveries, c.Stats())
	}
}

func TestSQSWorkerRetriesTransientFailureAndHonorsPriority(t *testing.T) {
	qs := sqsTestQueues(t, 30*time.Second, 5)
	c := NewSQSCoordinator(triage.PolicyAgent{}, nil, qs)
	ctx := context.Background()
	c.Submit(ctx, testJob(t, "low", 1, 8))
	c.Submit(ctx, testJob(t, "high", 5, 8))
	c.CloseSubmissions()

	var order []string
	failedOnce := false
	runner := func(job *queue.Job, attempt int) (inference.Result, error) {
		order = append(order, job.ID)
		if job.ID == "high" && !failedOnce {
			failedOnce = true
			return inference.Result{}, errors.New("transient GPU worker reset")
		}
		return inference.Result{}, nil
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := RunSQSWorker(wctx, qs, c, "w", runner); err != nil {
		t.Fatal(err)
	}
	// high first (fails), retried immediately, then low.
	if fmt.Sprint(order) != "[high high low]" {
		t.Fatalf("execution order = %v", order)
	}
	if s := c.Stats(); s.Succeeded != 2 || s.Retried != 1 || s.Redelivered != 0 {
		t.Fatalf("stats = %+v", s)
	}
}
