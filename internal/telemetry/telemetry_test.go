package telemetry

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewRecorderCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	rec, err := NewRecorder(path, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	defer rec.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s: %v", path, err)
	}
}

func TestRecordWritesJSONLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	rec, err := NewRecorder(path, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	now := time.Now()
	rec.Record(Event{Timestamp: now, JobID: "job-001", EventType: "submitted", Priority: 3, Detail: "cpu=2"})
	rec.Record(Event{Timestamp: now, JobID: "job-001", EventType: "started", NodeName: "node-a", Priority: 3})
	rec.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening written file: %v", err)
	}
	defer f.Close()

	var got []Event
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var e Event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			t.Fatalf("unmarshaling line %q: %v", scanner.Text(), err)
		}
		got = append(got, e)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0].EventType != "submitted" || got[0].JobID != "job-001" {
		t.Errorf("first event = %+v, want submitted/job-001", got[0])
	}
	if got[1].EventType != "started" || got[1].NodeName != "node-a" {
		t.Errorf("second event = %+v, want started/node-a", got[1])
	}
}

func TestRecordWithNilDBDoesNotPanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	rec, err := NewRecorder(path, nil)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	defer rec.Close()

	rec.Record(Event{JobID: "job-001", EventType: "submitted"})
}

func TestNewRecorderInvalidPath(t *testing.T) {
	_, err := NewRecorder(filepath.Join(t.TempDir(), "no-such-dir", "events.jsonl"), nil)
	if err == nil {
		t.Fatal("expected error creating file in nonexistent directory, got nil")
	}
}
