// Command tracer reads a scheduler event log (JSONL) and answers
// questions about a specific job's history: when it was submitted, every
// reason it was told to keep waiting, when (and where) it finally started,
// and its total wait time. This is the system-level "debugging complex
// issues" tool for the scheduler in cmd/scheduler.
//
//	tracer -log scheduling_events.jsonl -job job-017
//	tracer -log scheduling_events.jsonl -summary
//	tracer -log devices.jsonl -devices
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type Event struct {
	Timestamp time.Time `json:"timestamp"`
	JobID     string    `json:"job_id"`
	EventType string    `json:"event_type"`
	NodeName  string    `json:"node_name,omitempty"`
	Priority  int       `json:"priority"`
	Attempt   int       `json:"attempt,omitempty"`
	Version   int64     `json:"version,omitempty"`
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

// deviceRow is one device's state reconstructed from registry events
// (job IDs of the form "device:<id>", see internal/devices).
type deviceRow struct {
	id                   string
	desired, acked       int64
	attempts             int
	failedAt             int64 // version whose attempt budget was exhausted
	timeouts, dupIgnored int
	staleAcks            int
}

func (r *deviceRow) state() string {
	switch {
	case r.desired == 0:
		return "idle"
	case r.acked >= r.desired:
		return "acked"
	case r.failedAt == r.desired:
		return "failed"
	default:
		return "pending"
	}
}

// deviceStatus summarizes config-push delivery per device from the event log.
func deviceStatus(events []Event) []*deviceRow {
	rows := map[string]*deviceRow{}
	var order []string
	sorted := append([]Event(nil), events...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Timestamp.Before(sorted[j].Timestamp) })
	for _, e := range sorted {
		if !strings.HasPrefix(e.JobID, "device:") {
			continue
		}
		id := strings.TrimPrefix(e.JobID, "device:")
		r, ok := rows[id]
		if !ok {
			r = &deviceRow{id: id}
			rows[id] = r
			order = append(order, id)
		}
		switch e.EventType {
		case "config_set":
			r.desired = e.Version
			r.attempts = 0
		case "push":
			if e.Version == r.desired {
				r.attempts = e.Attempt
			}
		case "acked":
			if e.Version > r.acked {
				r.acked = e.Version
			}
		case "push_failed":
			r.failedAt = e.Version
		case "ack_timeout":
			r.timeouts++
		case "duplicate_ignored_by_device":
			r.dupIgnored++
		case "stale_ack_ignored":
			r.staleAcks++
		}
	}
	sort.Strings(order)
	out := make([]*deviceRow, 0, len(order))
	for _, id := range order {
		out = append(out, rows[id])
	}
	return out
}

func printDevices(events []Event) {
	rows := deviceStatus(events)
	if len(rows) == 0 {
		fmt.Println("no device registry events in this log")
		return
	}
	fmt.Printf("%-10s %-8s %-8s %-8s %-9s %-12s %-12s %s\n", "DEVICE", "DESIRED", "ACKED", "STATE", "ATTEMPTS", "ACK-TIMEOUTS", "DUP-IGNORED", "STALE-ACKS")
	for _, r := range rows {
		fmt.Printf("%-10s v%-7d v%-7d %-8s %-9d %-12d %-12d %d\n", r.id, r.desired, r.acked, r.state(), r.attempts, r.timeouts, r.dupIgnored, r.staleAcks)
	}
}

func main() {
	logPath := flag.String("log", "scheduling_events.jsonl", "path to the scheduler's JSONL event log")
	jobID := flag.String("job", "", "job ID to trace, e.g. job-017")
	showSummary := flag.Bool("summary", false, "print aggregate wait-time stats instead of tracing one job")
	showDevices := flag.Bool("devices", false, "print per-device config push status (from cmd/devices event logs)")
	flag.Parse()

	events, err := loadEvents(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tracer: %v\n", err)
		os.Exit(1)
	}

	if *showDevices {
		printDevices(events)
		return
	}
	if *showSummary {
		summary(events)
		return
	}
	if *jobID == "" {
		fmt.Fprintln(os.Stderr, "usage: tracer -log <path> (-job <id> | -summary | -devices)")
		os.Exit(1)
	}
	traceJob(events, *jobID)
}
