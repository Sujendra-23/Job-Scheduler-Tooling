// Command tracer reads a scheduler event log (JSONL) and answers
// questions about a specific job's history: when it was submitted, every
// reason it was told to keep waiting, when (and where) it finally started,
// and its total wait time. This is the system-level "debugging complex
// issues" tool for the scheduler in cmd/scheduler.
//
//	tracer -log scheduling_events.jsonl -job job-017
//	tracer -log scheduling_events.jsonl -summary
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"
)

type Event struct {
	Timestamp time.Time `json:"timestamp"`
	JobID     string    `json:"job_id"`
	EventType string    `json:"event_type"`
	NodeName  string    `json:"node_name,omitempty"`
	Priority  int       `json:"priority"`
	Detail    string    `json:"detail,omitempty"`
}

func loadEvents(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []Event
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var e Event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue // skip malformed lines rather than aborting the whole trace
		}
		events = append(events, e)
	}
	return events, scanner.Err()
}

func traceJob(events []Event, jobID string) {
	var timeline []Event
	for _, e := range events {
		if e.JobID == jobID {
			timeline = append(timeline, e)
		}
	}
	if len(timeline) == 0 {
		fmt.Printf("no events found for job %s\n", jobID)
		return
	}
	sort.Slice(timeline, func(i, j int) bool { return timeline[i].Timestamp.Before(timeline[j].Timestamp) })

	fmt.Printf("=== timeline for %s ===\n", jobID)
	var submitted, started time.Time
	waitReasons := 0
	for _, e := range timeline {
		fmt.Printf("  [%s] %-12s %s\n", e.Timestamp.Format(time.RFC3339), e.EventType, e.Detail)
		switch e.EventType {
		case "submitted":
			submitted = e.Timestamp
		case "started":
			started = e.Timestamp
		case "wait_reason":
			waitReasons++
		}
	}

	if !submitted.IsZero() && !started.IsZero() {
		fmt.Printf("\nsubmitted at %s, started at %s -> waited %s (told to wait %d times)\n",
			submitted.Format(time.RFC3339), started.Format(time.RFC3339), started.Sub(submitted), waitReasons)
	} else if !submitted.IsZero() {
		fmt.Printf("\nsubmitted at %s, never started in this log (still pending or preempted)\n", submitted.Format(time.RFC3339))
	}
}

// summary prints aggregate wait-time stats across every job in the log,
// the kind of thing you'd want when investigating "is scheduling latency
// getting worse" rather than one specific job.
func summary(events []Event) {
	type jobTimes struct {
		submitted, started time.Time
	}
	jobs := map[string]*jobTimes{}
	for _, e := range events {
		jt, ok := jobs[e.JobID]
		if !ok {
			jt = &jobTimes{}
			jobs[e.JobID] = jt
		}
		switch e.EventType {
		case "submitted":
			jt.submitted = e.Timestamp
		case "started":
			jt.started = e.Timestamp
		}
	}

	var waits []time.Duration
	for _, jt := range jobs {
		if !jt.submitted.IsZero() && !jt.started.IsZero() {
			waits = append(waits, jt.started.Sub(jt.submitted))
		}
	}
	if len(waits) == 0 {
		fmt.Println("no completed start events to summarize")
		return
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })

	var total time.Duration
	for _, w := range waits {
		total += w
	}
	fmt.Printf("=== wait time summary across %d jobs ===\n", len(waits))
	fmt.Printf("  min:    %s\n", waits[0])
	fmt.Printf("  median: %s\n", waits[len(waits)/2])
	fmt.Printf("  max:    %s\n", waits[len(waits)-1])
	fmt.Printf("  mean:   %s\n", total/time.Duration(len(waits)))
}

func main() {
	logPath := flag.String("log", "scheduling_events.jsonl", "path to the scheduler's JSONL event log")
	jobID := flag.String("job", "", "job ID to trace, e.g. job-017")
	showSummary := flag.Bool("summary", false, "print aggregate wait-time stats instead of tracing one job")
	flag.Parse()

	events, err := loadEvents(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tracer: %v\n", err)
		os.Exit(1)
	}

	if *showSummary {
		summary(events)
		return
	}
	if *jobID == "" {
		fmt.Fprintln(os.Stderr, "usage: tracer -log <path> (-job <id> | -summary)")
		os.Exit(1)
	}
	traceJob(events, *jobID)
}
