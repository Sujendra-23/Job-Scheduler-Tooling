package sqsqueue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"scheduler/internal/inference"
	"scheduler/internal/queue"
)

// Integration tests against LocalStack (or any SQS endpoint). They need
// SQS_ENDPOINT (e.g. http://localhost:4566) and skip otherwise.
func newTestQueues(t *testing.T, vis time.Duration, maxReceive int) *Queues {
	t.Helper()
	ep := os.Getenv("SQS_ENDPOINT")
	if ep == "" {
		t.Skip("SQS_ENDPOINT not set; skipping SQS integration test")
	}
	ctx := context.Background()
	q, err := Create(ctx, Config{
		Endpoint: ep, Prefix: fmt.Sprintf("t%d", time.Now().UnixNano()),
		VisibilityTimeout: vis, MaxReceiveCount: maxReceive,
	})
	if err != nil {
		t.Fatalf("create queues: %v", err)
	}
	t.Cleanup(func() { q.Delete(context.Background()) })
	return q
}

func job(t *testing.T, id string, prio int) *queue.Job {
	t.Helper()
	b, err := inference.NewResNet50Batch(8, "fp32", 1)
	if err != nil {
		t.Fatal(err)
	}
	return &queue.Job{ID: id, Priority: prio, Inference: b, MaxAttempts: 3, SubmitTime: time.Now()}
}

func TestReceiveServesHigherTierFirst(t *testing.T) {
	q := newTestQueues(t, 30*time.Second, 3)
	ctx := context.Background()
	for _, j := range []*queue.Job{job(t, "low", 1), job(t, "mid", 3), job(t, "high", 5)} {
		if err := q.Send(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	var order []string
	for i := 0; i < 3; i++ {
		d, err := q.Receive(ctx)
		if err != nil || d == nil {
			t.Fatalf("receive %d: d=%v err=%v", i, d, err)
		}
		order = append(order, d.Job.ID)
		q.Ack(ctx, d)
	}
	if fmt.Sprint(order) != "[high mid low]" {
		t.Fatalf("order = %v, want [high mid low]", order)
	}
	if d, _ := q.Receive(ctx); d != nil {
		t.Fatalf("queue should be empty, got %s", d.Job.ID)
	}
}

// A worker that receives a batch and then dies never acks it. The message must
// reappear after the visibility timeout with an incremented receive count.
func TestUnackedMessageIsRedeliveredAfterVisibilityTimeout(t *testing.T) {
	q := newTestQueues(t, 2*time.Second, 5)
	ctx := context.Background()
	q.Send(ctx, job(t, "j1", 3))
	first, _ := q.Receive(ctx)
	if first == nil || first.ReceiveCount != 1 {
		t.Fatalf("first delivery = %+v", first)
	}
	// "Worker dies": no Ack. While invisible, nobody else can take it.
	if d, _ := q.Receive(ctx); d != nil {
		t.Fatal("message should be invisible during the visibility timeout")
	}
	// Visibility expiry is not instantaneous on SQS/LocalStack: poll for it.
	start := time.Now()
	var second *Delivery
	for time.Since(start) < 8*time.Second && second == nil {
		time.Sleep(250 * time.Millisecond)
		second, _ = q.Receive(ctx)
	}
	if elapsed := time.Since(start); second != nil && elapsed < 1*time.Second {
		t.Fatalf("redelivered after only %s, before the 2s visibility timeout", elapsed)
	}
	if second == nil || second.Job.ID != "j1" || second.ReceiveCount != 2 {
		t.Fatalf("expected redelivery with receive count 2, got %+v", second)
	}
	q.Ack(ctx, second)
	time.Sleep(2500 * time.Millisecond)
	if d, _ := q.Receive(ctx); d != nil {
		t.Fatal("acked message must not be redelivered")
	}
}

func TestDeadLetteredAfterMaxReceiveCount(t *testing.T) {
	q := newTestQueues(t, 1*time.Second, 2)
	ctx := context.Background()
	q.Send(ctx, job(t, "poison", 4))
	for i := 1; i <= 2; i++ { // two deliveries, neither acked
		d, _ := q.Receive(ctx)
		if d == nil || d.ReceiveCount != i {
			t.Fatalf("delivery %d = %+v", i, d)
		}
		time.Sleep(1500 * time.Millisecond)
	}
	// Third attempt exceeds maxReceiveCount=2: SQS must route it to the DLQ
	// rather than hand it out again.
	deadline := time.Now().Add(10 * time.Second)
	var dead []DeadLetter
	for time.Now().Before(deadline) && len(dead) == 0 {
		if d, _ := q.Receive(ctx); d != nil {
			t.Fatalf("poison message delivered a 3rd time (receive count %d)", d.ReceiveCount)
		}
		dead, _ = q.PeekDeadLetters(ctx)
		time.Sleep(500 * time.Millisecond)
	}
	if len(dead) != 1 || dead[0].Job.ID != "poison" {
		t.Fatalf("DLQ contents = %+v", dead)
	}
}

func TestReleaseRedeliversImmediately(t *testing.T) {
	q := newTestQueues(t, 30*time.Second, 5)
	ctx := context.Background()
	q.Send(ctx, job(t, "r", 2))
	d, _ := q.Receive(ctx)
	if err := q.Release(ctx, d); err != nil {
		t.Fatal(err)
	}
	d2, _ := q.Receive(ctx)
	if d2 == nil || d2.ReceiveCount != 2 {
		t.Fatalf("expected immediate redelivery, got %+v", d2)
	}
}

func TestDeadLetterCarriesFailureClassAndDetail(t *testing.T) {
	q := newTestQueues(t, 30*time.Second, 3)
	ctx := context.Background()
	if err := q.DeadLetter(ctx, job(t, "sandboxed", 3), "timeout", "exceeded the 5s wall-clock limit"); err != nil {
		t.Fatal(err)
	}
	var dead []DeadLetter
	for i := 0; i < 20 && len(dead) == 0; i++ {
		var err error
		if dead, err = q.PeekDeadLetters(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(dead) != 1 || dead[0].Job.ID != "sandboxed" || dead[0].Class != "timeout" || dead[0].Detail != "exceeded the 5s wall-clock limit" {
		t.Fatalf("dead letters = %+v", dead)
	}
}
