package dispatch

import (
	"context"
	"fmt"
	"strings"

	"scheduler/internal/inference"
	"scheduler/internal/queue"
	"scheduler/internal/sandbox"
	"scheduler/internal/sqsqueue"
)

// WorkerOptions are optional worker capabilities.
type WorkerOptions struct {
	// Sandbox runs jobs that carry a Sandbox spec. When nil, such a job is
	// dead-lettered: an untrusted job is never run outside a sandbox.
	Sandbox sandbox.Runner
	// Logf receives one line per sandboxed job (optional).
	Logf func(format string, args ...any)
}

func (o WorkerOptions) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// sandboxQueue is the part of *sqsqueue.Queues a sandboxed job needs.
type sandboxQueue interface {
	Ack(ctx context.Context, d *sqsqueue.Delivery) error
	Release(ctx context.Context, d *sqsqueue.Delivery) error
	DeadLetter(ctx context.Context, job *queue.Job, class, detail string) error
}

// Dead-letter classes in addition to the sandbox.Class values.
const (
	DLQSandboxUnavailable = "sandbox_unavailable" // worker had no sandbox runner
	DLQInvalidSandboxSpec = "invalid_sandbox_spec"
)

// runSandboxedJob executes one delivery whose job carries a Sandbox spec.
//
//   - ok: report success to the coordinator, ack.
//   - any other verdict (non-zero exit, timeout, OOM kill, sandbox violation):
//     send the job to the dead-letter queue with the verdict, then ack. DLQ
//     first, so a crash in between redelivers the job instead of losing it.
//   - the sandbox could not run (Docker down, hardening not applied): the job
//     was not judged, so it is released for another worker and this worker
//     stops by returning the error.
func runSandboxedJob(ctx context.Context, q sandboxQueue, ctl Control, opts WorkerOptions, workerID string, d *sqsqueue.Delivery, attempt int) error {
	job := d.Job
	deadLetter := func(class, detail string) error {
		if err := q.DeadLetter(ctx, job, class, detail); err != nil {
			return fmt.Errorf("dead-letter %s: %w", job.ID, err)
		}
		return q.Ack(ctx, d)
	}

	if opts.Sandbox == nil {
		opts.logf("%s: %s refused: no sandbox runner on this worker", workerID, job.ID)
		return deadLetter(DLQSandboxUnavailable, "worker has no sandbox runner; refusing to run an untrusted job unsandboxed")
	}
	if err := job.Sandbox.Validate(); err != nil {
		return deadLetter(DLQInvalidSandboxSpec, err.Error())
	}

	out, err := opts.Sandbox.Run(ctx, *job.Sandbox)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = q.Release(ctx, d)
		return fmt.Errorf("sandbox cannot run %s: %w", job.ID, err)
	}
	opts.logf("%s: %s sandbox=%s exit=%d in %s %s", workerID, job.ID, out.Class, out.ExitCode, out.Duration.Round(1e6), strings.TrimSpace(out.Detail))

	if !out.Failed() {
		report := SQSReportArgs{
			WorkerID: workerID, Job: *job, Attempt: attempt, Success: true,
			Result: inference.Result{Backend: "docker-sandbox", Device: job.Sandbox.Image, LatencyMS: float64(out.Duration.Microseconds()) / 1000},
		}
		var rr SQSReportReply
		if err := ctl.Report(report, &rr); err != nil {
			return fmt.Errorf("report %s: %w", job.ID, err)
		}
		return q.Ack(ctx, d)
	}
	return deadLetter(string(out.Class), failureDetail(out))
}

func failureDetail(o sandbox.Outcome) string {
	detail := o.Detail
	if tail := strings.TrimSpace(o.Stderr); tail != "" {
		if len(tail) > 300 {
			tail = "…" + tail[len(tail)-300:]
		}
		detail += "; stderr: " + tail
	}
	return detail
}
