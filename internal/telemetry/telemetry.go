// Package telemetry records scheduling decisions as structured events, so
// a debugging tool can reconstruct why a specific job waited as long as it
// did. Events are always written to a local JSONL file; if a Postgres DSN
// is configured they're also inserted into a scheduling_events table for
// SQL-based analysis (see ../../sql/schema.sql).
package telemetry

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Event struct {
	Timestamp time.Time `json:"timestamp"`
	JobID     string    `json:"job_id"`
	EventType string    `json:"event_type"` // submitted, started, preempted, completed, wait_reason
	NodeName  string    `json:"node_name,omitempty"`
	Priority  int       `json:"priority"`
	Detail    string    `json:"detail,omitempty"`
}

type Recorder struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	db      *sql.DB // nil if no Postgres sink configured
}

func NewRecorder(jsonlPath string, db *sql.DB) (*Recorder, error) {
	f, err := os.Create(jsonlPath)
	if err != nil {
		return nil, fmt.Errorf("telemetry: creating %s: %w", jsonlPath, err)
	}
	return &Recorder{file: f, encoder: json.NewEncoder(f), db: db}, nil
}

func (r *Recorder) Record(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.encoder.Encode(e); err != nil {
		fmt.Fprintf(os.Stderr, "telemetry: write error: %v\n", err)
	}

	if r.db != nil {
		_, err := r.db.Exec(
			`INSERT INTO scheduling_events (timestamp, job_id, event_type, node_name, priority, detail)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			e.Timestamp, e.JobID, e.EventType, e.NodeName, e.Priority, e.Detail,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "telemetry: postgres insert error: %v\n", err)
		}
	}
}

func (r *Recorder) Close() error {
	return r.file.Close()
}
