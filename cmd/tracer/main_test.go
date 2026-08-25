package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// captureStdout redirects os.Stdout for the duration of fn and returns
// whatever it printed, so traceJob/summary (which print directly) can be
// tested without changing their signatures.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = orig

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("reading captured output: %v", err)
	}
	return buf.String()
}

func writeLog(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("writing test log: %v", err)
	}
	return path
}

func TestLoadEventsParsesValidLines(t *testing.T) {
	path := writeLog(t,
		`{"timestamp":"2026-01-01T00:00:00Z","job_id":"job-001","event_type":"submitted","priority":3}`,
		`{"timestamp":"2026-01-01T00:00:05Z","job_id":"job-001","event_type":"started","node_name":"node-a","priority":3}`,
	)

	events, err := loadEvents(path)
	if err != nil {
		t.Fatalf("loadEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].EventType != "submitted" || events[1].EventType != "started" {
		t.Errorf("unexpected event order/content: %+v", events)
	}
}

func TestLoadEventsSkipsMalformedLines(t *testing.T) {
	path := writeLog(t,
		`{"timestamp":"2026-01-01T00:00:00Z","job_id":"job-001","event_type":"submitted","priority":3}`,
		`not valid json`,
		`{"timestamp":"2026-01-01T00:00:05Z","job_id":"job-002","event_type":"submitted","priority":1}`,
	)

	events, err := loadEvents(path)
	if err != nil {
		t.Fatalf("loadEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (malformed line should be skipped): %+v", len(events), events)
	}
}

func TestLoadEventsMissingFile(t *testing.T) {
	_, err := loadEvents(filepath.Join(t.TempDir(), "does-not-exist.jsonl"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestTraceJobReportsWaitTime(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []Event{
		{Timestamp: base, JobID: "job-001", EventType: "submitted", Priority: 3},
		{Timestamp: base.Add(9 * time.Second), JobID: "job-001", EventType: "started", NodeName: "node-a", Priority: 3},
		{Timestamp: base.Add(20 * time.Second), JobID: "job-001", EventType: "completed", NodeName: "node-a", Priority: 3},
	}

	out := captureStdout(t, func() { traceJob(events, "job-001") })

	if !strings.Contains(out, "waited 9s") {
		t.Errorf("expected output to report a 9s wait, got:\n%s", out)
	}
	if !strings.Contains(out, "told to wait 0 times") {
		t.Errorf("expected 0 wait_reason events, got:\n%s", out)
	}
}

func TestTraceJobUnknownJob(t *testing.T) {
	out := captureStdout(t, func() { traceJob(nil, "job-999") })
	if !strings.Contains(out, "no events found for job job-999") {
		t.Errorf("expected 'no events found' message, got:\n%s", out)
	}
}

func TestSummaryComputesWaitStats(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []Event{
		{Timestamp: base, JobID: "job-001", EventType: "submitted"},
		{Timestamp: base.Add(2 * time.Second), JobID: "job-001", EventType: "started"},
		{Timestamp: base, JobID: "job-002", EventType: "submitted"},
		{Timestamp: base.Add(10 * time.Second), JobID: "job-002", EventType: "started"},
	}

	out := captureStdout(t, func() { summary(events) })

	if !strings.Contains(out, "across 2 jobs") {
		t.Errorf("expected summary across 2 jobs, got:\n%s", out)
	}
	if !strings.Contains(out, "min:    2s") {
		t.Errorf("expected min wait 2s, got:\n%s", out)
	}
	if !strings.Contains(out, "max:    10s") {
		t.Errorf("expected max wait 10s, got:\n%s", out)
	}
}

func TestSummaryNoCompletedStarts(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []Event{
		{Timestamp: base, JobID: "job-001", EventType: "submitted"},
	}

	out := captureStdout(t, func() { summary(events) })
	if !strings.Contains(out, "no completed start events to summarize") {
		t.Errorf("expected 'no completed start events' message, got:\n%s", out)
	}
}
