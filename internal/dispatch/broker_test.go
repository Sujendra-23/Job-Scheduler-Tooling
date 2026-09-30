package dispatch

import (
	"testing"
	"time"

	"scheduler/internal/inference"
	"scheduler/internal/queue"
	"scheduler/internal/triage"
)

func testJob(t *testing.T, id string, priority, batchSize int) *queue.Job {
	t.Helper()
	b, err := inference.NewResNet50Batch(batchSize, "fp32", 1)
	if err != nil {
		t.Fatal(err)
	}
	return &queue.Job{ID: id, Priority: priority, Inference: b, MaxAttempts: 3, SubmitTime: time.Now()}
}

func TestBrokerClaimsByPriorityAcrossWorkers(t *testing.T) {
	b := NewBroker(triage.PolicyAgent{}, nil)
	b.Submit(testJob(t, "low", 1, 8))
	b.Submit(testJob(t, "high", 5, 8))
	b.CloseSubmissions()

	var first, second ClaimReply
	if err := b.Claim(ClaimArgs{WorkerID: "worker-1"}, &first); err != nil {
		t.Fatal(err)
	}
	if err := b.Claim(ClaimArgs{WorkerID: "worker-2"}, &second); err != nil {
		t.Fatal(err)
	}
	if first.Job.ID != "high" || second.Job.ID != "low" {
		t.Fatalf("claim order = %s, %s", first.Job.ID, second.Job.ID)
	}
	if first.Job.Node == second.Job.Node {
		t.Fatal("jobs should be assigned to distinct requesting workers")
	}
}

func TestBrokerOOMIsAdjustedAndRequeued(t *testing.T) {
	b := NewBroker(triage.PolicyAgent{}, nil)
	b.Submit(testJob(t, "oom", 3, 32))
	b.CloseSubmissions()
	var claim ClaimReply
	b.Claim(ClaimArgs{WorkerID: "worker-1"}, &claim)
	var report ReportReply
	if err := b.Report(ReportArgs{WorkerID: "worker-1", JobID: "oom", Error: "CUDA out of memory"}, &report); err != nil {
		t.Fatal(err)
	}
	if report.Decision.Action != triage.AutoAdjust {
		t.Fatalf("decision = %+v", report.Decision)
	}
	var adjusted ClaimReply
	b.Claim(ClaimArgs{WorkerID: "worker-2"}, &adjusted)
	if adjusted.Job.Inference.BatchSize != 16 || adjusted.Job.Inference.Precision != "fp16" {
		t.Fatalf("adjusted batch = %+v", adjusted.Job.Inference)
	}
}

func TestBrokerCompletesAndSignalsDone(t *testing.T) {
	b := NewBroker(triage.PolicyAgent{}, nil)
	b.Submit(testJob(t, "ok", 3, 8))
	b.CloseSubmissions()
	var claim ClaimReply
	b.Claim(ClaimArgs{WorkerID: "worker-1"}, &claim)
	if err := b.Report(ReportArgs{WorkerID: "worker-1", JobID: "ok", Success: true}, &ReportReply{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.Done():
	case <-time.After(time.Second):
		t.Fatal("broker did not signal completion")
	}
}
