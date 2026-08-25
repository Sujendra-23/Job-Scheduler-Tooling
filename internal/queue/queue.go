// Package queue implements a priority queue of scheduling jobs, with
// support for preempting a lower-priority running job when a higher-
// priority job can't otherwise be placed.
package queue

import (
	"container/heap"
	"time"

	"scheduler/internal/resource"
)

type JobState int

const (
	Pending JobState = iota
	Running
	Preempted
	Completed
)

func (s JobState) String() string {
	switch s {
	case Pending:
		return "pending"
	case Running:
		return "running"
	case Preempted:
		return "preempted"
	case Completed:
		return "completed"
	default:
		return "unknown"
	}
}

type Job struct {
	ID        string
	Priority  int // higher = more important
	Resources resource.Requirements
	State     JobState
	SubmitTime time.Time
	StartTime  time.Time // zero until it starts running
	Node       string    // which node it's running on, once placed

	index int // heap bookkeeping, do not set directly
}

// WaitTime returns how long a job sat pending before it started running
// (or, if it hasn't started yet, how long it's been waiting so far).
func (j *Job) WaitTime(now time.Time) time.Duration {
	if !j.StartTime.IsZero() {
		return j.StartTime.Sub(j.SubmitTime)
	}
	return now.Sub(j.SubmitTime)
}

// priorityHeap implements container/heap.Interface, ordering by Priority
// (descending) then by SubmitTime (ascending, i.e. FIFO within a priority
// tier - this is what prevents starvation among equal-priority jobs).
type priorityHeap []*Job

func (h priorityHeap) Len() int { return len(h) }
func (h priorityHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].SubmitTime.Before(h[j].SubmitTime)
}
func (h priorityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *priorityHeap) Push(x interface{}) {
	job := x.(*Job)
	job.index = len(*h)
	*h = append(*h, job)
}
func (h *priorityHeap) Pop() interface{} {
	old := *h
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	job.index = -1
	*h = old[:n-1]
	return job
}

// Queue wraps priorityHeap with the operations a scheduler actually needs.
type Queue struct {
	heap priorityHeap
}

func New() *Queue {
	q := &Queue{}
	heap.Init(&q.heap)
	return q
}

func (q *Queue) Push(j *Job) {
	heap.Push(&q.heap, j)
}

// Pop removes and returns the highest-priority (then oldest) pending job,
// or nil if the queue is empty.
func (q *Queue) Pop() *Job {
	if q.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(&q.heap).(*Job)
}

func (q *Queue) Len() int { return q.heap.Len() }

// Peek returns the next job without removing it, or nil if empty.
func (q *Queue) Peek() *Job {
	if q.heap.Len() == 0 {
		return nil
	}
	return q.heap[0]
}
