package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"scheduler/internal/queue"
	"scheduler/internal/sandbox"
	"scheduler/internal/sqsqueue"
	"scheduler/internal/telemetry"
	"scheduler/internal/triage"
)

// ---- unit tests: fake runner, fake queue, fake coordinator (no Docker, no SQS) ----

type fakeRunner struct {
	out   sandbox.Outcome
	err   error
	specs []sandbox.Spec
}

func (f *fakeRunner) Run(_ context.Context, s sandbox.Spec) (sandbox.Outcome, error) {
	f.specs = append(f.specs, s)
	return f.out, f.err
}

type deadLetter struct{ id, class, detail string }

type fakeQueue struct {
	acked, released int
	dead            []deadLetter
	order           []string
	dlqErr          error
}

func (q *fakeQueue) Ack(context.Context, *sqsqueue.Delivery) error {
	q.acked++
	q.order = append(q.order, "ack")
	return nil
}
func (q *fakeQueue) Release(context.Context, *sqsqueue.Delivery) error { q.released++; return nil }
func (q *fakeQueue) DeadLetter(_ context.Context, j *queue.Job, class, detail string) error {
	q.order = append(q.order, "dlq")
	if q.dlqErr != nil {
		return q.dlqErr
	}
	q.dead = append(q.dead, deadLetter{j.ID, class, detail})
	return nil
}

type fakeControl struct{ reports []SQSReportArgs }

func (c *fakeControl) Started(StartedArgs, *StartedReply) error { return nil }
func (c *fakeControl) Report(a SQSReportArgs, _ *SQSReportReply) error {
	c.reports = append(c.reports, a)
	return nil
}
func (c *fakeControl) Status(StatusArgs, *StatusReply) error { return nil }

func sandboxJob(id string) *queue.Job {
	return &queue.Job{ID: id, Priority: 3, MaxAttempts: 3, SubmitTime: time.Now(),
		Sandbox: &sandbox.Spec{Image: "alpine:3.20", Cmd: []string{"true"}, CPUs: 0.5, MemoryMB: 64, TimeoutS: 5}}
}

func runOne(t *testing.T, r sandbox.Runner, q *fakeQueue, ctl *fakeControl, job *queue.Job) error {
	t.Helper()
	return runSandboxedJob(context.Background(), q, ctl, WorkerOptions{Sandbox: r}, "w1", &sqsqueue.Delivery{Job: job}, 1)
}

func TestSandboxedSuccessIsReportedAndAckedNotDeadLettered(t *testing.T) {
	q, ctl := &fakeQueue{}, &fakeControl{}
	r := &fakeRunner{out: sandbox.Outcome{Class: sandbox.ClassOK, Duration: 40 * time.Millisecond}}
	if err := runOne(t, r, q, ctl, sandboxJob("ok")); err != nil {
		t.Fatal(err)
	}
	if len(q.dead) != 0 || q.acked != 1 || len(ctl.reports) != 1 || !ctl.reports[0].Success {
		t.Fatalf("dead=%v acked=%d reports=%+v", q.dead, q.acked, ctl.reports)
	}
	if ctl.reports[0].Result.Backend != "docker-sandbox" {
		t.Fatalf("result = %+v", ctl.reports[0].Result)
	}
}

// Every way a sandboxed run can go wrong must land in the DLQ with its class.
func TestEveryFailureClassLandsInTheDeadLetterQueue(t *testing.T) {
	cases := []struct {
		name string
		out  sandbox.Outcome
	}{
		{"non-zero exit", sandbox.Outcome{Class: sandbox.ClassNonZeroExit, ExitCode: 3, Detail: "exited with status 3", Stderr: "boom"}},
		{"timeout", sandbox.Outcome{Class: sandbox.ClassTimeout, ExitCode: -1, Detail: "exceeded the 5s wall-clock limit"}},
		{"oom kill", sandbox.Outcome{Class: sandbox.ClassOOMKilled, ExitCode: 137, Detail: "killed by the kernel OOM killer"}},
		{"escape attempt", sandbox.Outcome{Class: sandbox.ClassSandboxViolation, ExitCode: 1, Detail: "tried to use the network (disabled)"}},
	}
	for _, c := range cases {
		q, ctl := &fakeQueue{}, &fakeControl{}
		if err := runOne(t, &fakeRunner{out: c.out}, q, ctl, sandboxJob("j")); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(q.dead) != 1 || q.dead[0].class != string(c.out.Class) || !strings.Contains(q.dead[0].detail, c.out.Detail) {
			t.Errorf("%s: dead=%+v", c.name, q.dead)
		}
		// DLQ strictly before ack, so a crash in between redelivers rather than loses the job.
		if fmt.Sprint(q.order) != "[dlq ack]" {
			t.Errorf("%s: order = %v", c.name, q.order)
		}
		// Not retried through triage: a sandbox verdict is final.
		if len(ctl.reports) != 0 {
			t.Errorf("%s: failure must not be reported for triage/retry: %+v", c.name, ctl.reports)
		}
	}
}

func TestDeadLetterDetailIncludesStderrTail(t *testing.T) {
	q := &fakeQueue{}
	out := sandbox.Outcome{Class: sandbox.ClassNonZeroExit, Detail: "exited with status 1", Stderr: strings.Repeat("x", 1000) + "THE-END"}
	runOne(t, &fakeRunner{out: out}, q, &fakeControl{}, sandboxJob("j"))
	if d := q.dead[0].detail; !strings.HasSuffix(d, "THE-END") || len(d) > 400 {
		t.Fatalf("detail = %q", d)
	}
}

func TestFailedDeadLetterWriteDoesNotAck(t *testing.T) {
	q := &fakeQueue{dlqErr: errors.New("sqs down")}
	err := runOne(t, &fakeRunner{out: sandbox.Outcome{Class: sandbox.ClassTimeout}}, q, &fakeControl{}, sandboxJob("j"))
	if err == nil || q.acked != 0 {
		t.Fatalf("err=%v acked=%d: the job must stay on the queue if it could not be dead-lettered", err, q.acked)
	}
}

func TestWorkerWithoutASandboxRefusesToRunUntrustedJobs(t *testing.T) {
	q := &fakeQueue{}
	err := runSandboxedJob(context.Background(), q, &fakeControl{}, WorkerOptions{}, "w1", &sqsqueue.Delivery{Job: sandboxJob("j")}, 1)
	if err != nil || len(q.dead) != 1 || q.dead[0].class != DLQSandboxUnavailable {
		t.Fatalf("err=%v dead=%+v", err, q.dead)
	}
}

func TestInvalidSpecIsDeadLetteredWithoutRunning(t *testing.T) {
	q, r := &fakeQueue{}, &fakeRunner{}
	j := sandboxJob("j")
	j.Sandbox.Image = "--privileged"
	if err := runOne(t, r, q, &fakeControl{}, j); err != nil {
		t.Fatal(err)
	}
	if len(r.specs) != 0 || len(q.dead) != 1 || q.dead[0].class != DLQInvalidSandboxSpec {
		t.Fatalf("ran=%d dead=%+v", len(r.specs), q.dead)
	}
}

func TestSandboxInfrastructureFailureIsNotAJobFailure(t *testing.T) {
	q := &fakeQueue{}
	err := runOne(t, &fakeRunner{err: errors.New("docker daemon not running")}, q, &fakeControl{}, sandboxJob("j"))
	if err == nil || len(q.dead) != 0 || q.acked != 0 || q.released != 1 {
		t.Fatalf("err=%v dead=%v acked=%d released=%d: the job must be released, not judged", err, q.dead, q.acked, q.released)
	}
}

func TestSubmitValidatesSandboxJobsAndBrokerRefusesThem(t *testing.T) {
	c := NewSQSCoordinator(triage.PolicyAgent{}, nil, nil)
	bad := sandboxJob("bad")
	bad.Sandbox.MemoryMB = 0
	if err := c.Submit(context.Background(), bad); err == nil {
		t.Fatal("invalid sandbox spec must be rejected at submit")
	}
	if err := NewBroker(triage.PolicyAgent{}, nil).Submit(sandboxJob("x")); err == nil {
		t.Fatal("the in-process broker has no DLQ and must refuse sandboxed jobs")
	}
}

// ---- integration: real Docker + SQS (LocalStack) ----

// Submits sandboxed jobs that exit 0 / exit 3 / run too long / eat memory /
// touch the network, runs a real worker with the real DockerRunner against
// LocalStack, and checks that every failure is in the DLQ with its class.
// Needs SQS_ENDPOINT and a Docker daemon; skips otherwise.
func TestSandboxedJobFailuresLandInTheRealDLQ(t *testing.T) {
	qs := sqsTestQueues(t, 60*time.Second, 3) // skips without SQS_ENDPOINT
	if os.Getenv("SANDBOX_SKIP_DOCKER") != "" {
		t.Skip("SANDBOX_SKIP_DOCKER set")
	}
	if err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}").Run(); err != nil {
		t.Skip("docker daemon not reachable")
	}
	image := "alpine:3.20"
	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		if out, err := exec.Command("docker", "pull", "-q", image).CombinedOutput(); err != nil {
			t.Skipf("cannot pull %s: %v: %s", image, err, out)
		}
	}

	spec := func(script string, timeout int) *sandbox.Spec {
		return &sandbox.Spec{Image: image, Cmd: []string{"sh", "-c", script}, CPUs: 0.5, MemoryMB: 64, TimeoutS: timeout}
	}
	wantClass := map[string]string{
		"good": "", "exit3": "non_zero_exit", "slow": "timeout", "hog": "oom_killed", "net": "sandbox_violation", "rofs": "sandbox_violation",
	}
	jobs := map[string]*sandbox.Spec{
		"good": spec("echo fine", 20), "exit3": spec("exit 3", 20), "slow": spec("sleep 60", 2),
		"hog": spec("tail /dev/zero", 20), "net": spec("wget -T 3 -q -O- http://1.1.1.1/", 20), "rofs": spec("echo x > /etc/x", 20),
	}

	eventsPath := filepath.Join(t.TempDir(), "events.jsonl")
	rec, err := telemetry.NewRecorder(eventsPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := NewSQSCoordinator(triage.PolicyAgent{}, rec, qs)
	ctx, stop := context.WithTimeout(context.Background(), 120*time.Second)
	defer stop()
	for id, s := range jobs {
		j := &queue.Job{ID: id, Priority: 3, MaxAttempts: 3, SubmitTime: time.Now(), Sandbox: s}
		if err := c.Submit(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	c.CloseSubmissions()
	go c.WatchDeadLetters(ctx, 300*time.Millisecond)

	if err := RunSQSWorkerWith(ctx, qs, c, "w1", nil, WorkerOptions{Sandbox: &sandbox.DockerRunner{}, Logf: t.Logf}); err != nil {
		t.Fatalf("worker: %v", err)
	}
	select {
	case <-c.Done():
	case <-time.After(20 * time.Second):
		t.Fatalf("coordinator never reached a terminal state for every job: %+v", c.Stats())
	}
	if s := c.Stats(); s.Succeeded != 1 || s.DeadLettered != 5 {
		t.Fatalf("stats = %+v", s)
	}

	// The coordinator read each failure back out of the real DLQ: its telemetry names the class.
	rec.Close()
	raw, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	dead := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e telemetry.Event
		if json.Unmarshal([]byte(line), &e) == nil && e.EventType == "dead_lettered" {
			dead[e.JobID] = e.Detail
		}
	}
	for id, want := range wantClass {
		detail, inDLQ := dead[id]
		switch {
		case want == "" && inDLQ:
			t.Errorf("%s succeeded and must not be dead-lettered", id)
		case want != "" && !strings.HasPrefix(detail, want+":"):
			t.Errorf("%s: dead_lettered detail = %q, want class %q", id, detail, want)
		}
	}
}
