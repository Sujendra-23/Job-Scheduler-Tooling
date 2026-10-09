package sandbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	labelSandbox  = "scheduler.sandbox"
	labelDeadline = "scheduler.sandbox.deadline" // unix seconds; Sweep removes containers past it
	outputLimit   = 64 << 10
)

// ExecFunc runs `docker <args>` and returns its exit code. A non-nil error
// means docker could not be run at all. Tests replace it with a fake.
type ExecFunc func(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) (int, error)

// DockerRunner runs jobs with the docker CLI. The CLI (not an SDK) keeps the
// module dependency-free and makes the exact hardening flags easy to read.
type DockerRunner struct {
	Bin  string   // default "docker"
	Exec ExecFunc // default: os/exec
	// Now is injectable for tests.
	Now func() time.Time
}

func (r *DockerRunner) bin() string {
	if r.Bin != "" {
		return r.Bin
	}
	return "docker"
}

func (r *DockerRunner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *DockerRunner) docker(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) (int, error) {
	if r.Exec != nil {
		return r.Exec(ctx, stdin, stdout, stderr, args...)
	}
	cmd := exec.CommandContext(ctx, r.bin(), args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), nil
	}
	if err != nil {
		return -1, err
	}
	return 0, nil
}

// out runs a short docker command and returns its trimmed stdout.
func (r *DockerRunner) out(ctx context.Context, args ...string) (string, error) {
	var so, se bytes.Buffer
	code, err := r.docker(ctx, nil, &so, &se, args...)
	if err != nil {
		return "", fmt.Errorf("docker %s: %w", args[0], err)
	}
	if code != 0 {
		return "", fmt.Errorf("docker %s: exit %d: %s", args[0], code, strings.TrimSpace(se.String()))
	}
	return strings.TrimSpace(so.String()), nil
}

// capWriter keeps the first limit bytes and counts the rest, so a job that
// prints forever cannot exhaust the worker's memory.
type capWriter struct {
	buf     bytes.Buffer
	limit   int
	dropped int
}

func (w *capWriter) Write(p []byte) (int, error) {
	if room := w.limit - w.buf.Len(); room > 0 {
		n := len(p)
		if n > room {
			n = room
		}
		w.buf.Write(p[:n])
		w.dropped += len(p) - n
	} else {
		w.dropped += len(p)
	}
	return len(p), nil
}

func (w *capWriter) String() string {
	if w.dropped > 0 {
		return w.buf.String() + fmt.Sprintf("\n…[%d more bytes dropped]", w.dropped)
	}
	return w.buf.String()
}

// CreateArgs is the complete, reviewed `docker create` command line for a
// spec. Every flag here is part of the isolation contract and is re-checked
// against `docker inspect` before the container is started (verifyHardening).
func CreateArgs(spec Spec, name string, deadline time.Time) []string {
	mem := strconv.Itoa(spec.MemoryMB) + "m"
	args := []string{
		"create", "--name", name,
		"--label", labelSandbox + "=1",
		"--label", labelDeadline + "=" + strconv.FormatInt(deadline.Unix(), 10),
		"--network", "none",
		"--read-only",
		"--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m",
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges",
		"--memory", mem, "--memory-swap", mem, // equal values: no swap
		"--cpus", strconv.FormatFloat(spec.CPUs, 'f', -1, 64),
		"--pids-limit", strconv.Itoa(spec.pids()),
		"--ulimit", "nofile=256:256", "--ulimit", "core=0",
		"--user", "65534:65534",
		"--workdir", "/tmp",
	}
	if spec.Stdin != "" {
		args = append(args, "-i")
	}
	for k, v := range spec.Env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, spec.Image)
	return append(args, spec.Cmd...)
}

type inspectDoc struct {
	HostConfig struct {
		NetworkMode    string
		ReadonlyRootfs bool
		CapDrop        []string
		Privileged     bool
		Memory         int64
		MemorySwap     int64
		NanoCpus       int64
		PidsLimit      *int64
		SecurityOpt    []string
	}
	State struct {
		ExitCode  int
		OOMKilled bool
		Error     string
	}
}

// verifyHardening fails closed: if Docker did not apply a boundary we rely on
// (a daemon default, a wrapper script, a changed flag), the job does not run.
func verifyHardening(d inspectDoc, spec Spec) error {
	h := d.HostConfig
	var bad []string
	if h.NetworkMode != "none" {
		bad = append(bad, "network mode is "+h.NetworkMode)
	}
	if !h.ReadonlyRootfs {
		bad = append(bad, "root filesystem is writable")
	}
	if len(h.CapDrop) != 1 || !strings.EqualFold(h.CapDrop[0], "ALL") {
		bad = append(bad, fmt.Sprintf("capabilities not dropped (%v)", h.CapDrop))
	}
	if h.Privileged {
		bad = append(bad, "container is privileged")
	}
	if h.Memory != int64(spec.MemoryMB)<<20 || h.MemorySwap != h.Memory {
		bad = append(bad, "memory/swap limit not applied")
	}
	if h.NanoCpus != int64(spec.CPUs*1e9+0.5) {
		bad = append(bad, "cpu limit not applied")
	}
	if h.PidsLimit == nil || *h.PidsLimit != int64(spec.pids()) {
		bad = append(bad, "pids limit not applied")
	}
	nnp := false
	for _, o := range h.SecurityOpt {
		if strings.HasPrefix(o, "no-new-privileges") {
			nnp = true
		}
	}
	if !nnp {
		bad = append(bad, "no-new-privileges not set")
	}
	if len(bad) > 0 {
		return fmt.Errorf("sandbox hardening not in effect, refusing to run: %s", strings.Join(bad, "; "))
	}
	return nil
}

// Run implements Runner.
func (r *DockerRunner) Run(ctx context.Context, spec Spec) (Outcome, error) {
	if err := spec.Validate(); err != nil {
		return Outcome{}, err
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return Outcome{}, err
	}
	name := "sandbox-" + hex.EncodeToString(rnd[:])

	if _, err := r.out(ctx, CreateArgs(spec, name, r.now().Add(spec.Timeout()+time.Minute))...); err != nil {
		return Outcome{}, err
	}
	// Always remove, even if ctx is cancelled: a leaked container keeps its CPU and memory.
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = r.out(cctx, "rm", "-f", name)
	}()

	raw, err := r.out(ctx, "inspect", "--type", "container", name)
	if err != nil {
		return Outcome{}, err
	}
	var docs []inspectDoc
	if err := json.Unmarshal([]byte(raw), &docs); err != nil || len(docs) != 1 {
		return Outcome{}, fmt.Errorf("sandbox: unreadable docker inspect output: %v", err)
	}
	if err := verifyHardening(docs[0], spec); err != nil {
		return Outcome{}, err
	}

	runCtx, cancel := context.WithTimeout(ctx, spec.Timeout())
	defer cancel()
	stdout, stderr := &capWriter{limit: outputLimit}, &capWriter{limit: outputLimit}
	startArgs := []string{"start", "-a"}
	var stdin io.Reader
	if spec.Stdin != "" {
		startArgs = append(startArgs, "-i")
		stdin = strings.NewReader(spec.Stdin)
	}
	began := r.now()
	_, startErr := r.docker(runCtx, stdin, stdout, stderr, append(startArgs, name)...)
	elapsed := r.now().Sub(began)

	if ctx.Err() != nil { // the caller gave up; this is not a verdict on the job
		r.kill(name)
		return Outcome{}, ctx.Err()
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		// Killing the docker client does not stop the container; kill it explicitly.
		r.kill(name)
		return Outcome{
			Class: ClassTimeout, ExitCode: -1, Stdout: stdout.String(), Stderr: stderr.String(), Duration: elapsed,
			Detail: fmt.Sprintf("exceeded the %s wall-clock limit and was killed", spec.Timeout()),
		}, nil
	}
	if startErr != nil {
		return Outcome{}, fmt.Errorf("docker start: %w", startErr)
	}

	raw, err = r.out(ctx, "inspect", "--type", "container", name)
	if err != nil {
		return Outcome{}, err
	}
	if err := json.Unmarshal([]byte(raw), &docs); err != nil || len(docs) != 1 {
		return Outcome{}, fmt.Errorf("sandbox: unreadable docker inspect output: %v", err)
	}
	st := docs[0].State
	if st.Error != "" && st.ExitCode != 0 && stdout.buf.Len() == 0 && stderr.buf.Len() == 0 {
		// e.g. "exec: not found" / "permission denied": the command could not start. A job problem, not a docker one.
		stderr.Write([]byte(st.Error))
	}
	class, detail := ClassifyExit(st.ExitCode, st.OOMKilled, stderr.String())
	return Outcome{
		Class: class, ExitCode: st.ExitCode, Stdout: stdout.String(), Stderr: stderr.String(),
		Duration: elapsed, Detail: detail,
	}, nil
}

func (r *DockerRunner) kill(name string) {
	cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = r.out(cctx, "kill", name)
}

// Sweep removes sandbox containers that outlived their deadline. A worker that
// is SIGKILLed mid-job takes its docker client with it but not the container;
// workers call Sweep on startup so those do not run forever.
func (r *DockerRunner) Sweep(ctx context.Context) (int, error) {
	listing, err := r.out(ctx, "ps", "-a", "--filter", "label="+labelSandbox+"=1", "--format",
		`{{.ID}} {{.Label "`+labelDeadline+`"}}`)
	if err != nil || listing == "" {
		return 0, err
	}
	removed := 0
	for _, line := range strings.Split(listing, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		deadline, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil || r.now().Unix() <= deadline {
			continue
		}
		if _, err := r.out(ctx, "rm", "-f", f[0]); err == nil {
			removed++
		}
	}
	return removed, nil
}
