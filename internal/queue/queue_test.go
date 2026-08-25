package queue

import (
	"testing"
	"time"
)

func TestPopReturnsHighestPriorityFirst(t *testing.T) {
	q := New()
	now := time.Now()
	q.Push(&Job{ID: "low", Priority: 1, SubmitTime: now})
	q.Push(&Job{ID: "high", Priority: 5, SubmitTime: now})
	q.Push(&Job{ID: "mid", Priority: 3, SubmitTime: now})

	order := []string{}
	for q.Len() > 0 {
		order = append(order, q.Pop().ID)
	}

	want := []string{"high", "mid", "low"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("pop order = %v, want %v", order, want)
		}
	}
}

func TestEqualPriorityIsFIFO(t *testing.T) {
	q := New()
	base := time.Now()
	q.Push(&Job{ID: "second", Priority: 2, SubmitTime: base.Add(time.Second)})
	q.Push(&Job{ID: "first", Priority: 2, SubmitTime: base})
	q.Push(&Job{ID: "third", Priority: 2, SubmitTime: base.Add(2 * time.Second)})

	first := q.Pop()
	if first.ID != "first" {
		t.Fatalf("expected earliest-submitted job first among equal priority, got %s", first.ID)
	}
}

func TestPopOnEmptyQueueReturnsNil(t *testing.T) {
	q := New()
	if got := q.Pop(); got != nil {
		t.Fatalf("expected nil from empty queue, got %v", got)
	}
}

func TestPeekDoesNotRemove(t *testing.T) {
	q := New()
	q.Push(&Job{ID: "a", Priority: 1, SubmitTime: time.Now()})
	before := q.Len()
	q.Peek()
	if q.Len() != before {
		t.Fatalf("Peek should not change queue length: before=%d after=%d", before, q.Len())
	}
}
