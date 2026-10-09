package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func goodSpec() Spec {
	return Spec{Image: "alpine:3.20", Cmd: []string{"echo", "hi"}, CPUs: 0.5, MemoryMB: 64, TimeoutS: 5}
}

func TestValidate(t *testing.T) {
	cases := map[string]func(*Spec){
		"flag-injection image": func(s *Spec) { s.Image = "--privileged" },
		"empty image":          func(s *Spec) { s.Image = "" },
		"no command":           func(s *Spec) { s.Cmd = nil },
		"zero cpus":            func(s *Spec) { s.CPUs = 0 },
		"too many cpus":        func(s *Spec) { s.CPUs = 64 },
		"tiny memory":          func(s *Spec) { s.MemoryMB = 1 },
		"huge memory":          func(s *Spec) { s.MemoryMB = 1 << 20 },
		"no timeout":           func(s *Spec) { s.TimeoutS = 0 },
		"day-long timeout":     func(s *Spec) { s.TimeoutS = 86400 },
		"bad env name":         func(s *Spec) { s.Env = map[string]string{"A=B --privileged": "x"} },
	}
	for name, mutate := range cases {
		s := goodSpec()
		mutate(&s)
		if err := s.Validate(); err == nil {
			t.Errorf("%s: want validation error", name)
		}
	}
	if err := goodSpec().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestClassifyExit(t *testing.T) {
	cases := []struct {
		name   string
		code   int
		oom    bool
		stderr string
		want   Class
	}{
		{"ok", 0, false, "", ClassOK},
		{"plain failure", 3, false, "boom", ClassNonZeroExit},
		{"oom wins over 137", 137, true, "", ClassOOMKilled},
		{"sigkill without oom is a plain failure", 137, false, "", ClassNonZeroExit},
		{"seccomp", 159, false, "", ClassSandboxViolation},
		{"write to ro rootfs", 1, false, "touch: /x: Read-only file system", ClassSandboxViolation},
		{"network attempt", 1, false, "wget: can't connect to remote host: Network unreachable", ClassSandboxViolation},
		{"dns attempt", 1, false, "wget: bad address 'example.com'", ClassSandboxViolation},
		{"privileged op", 1, false, "mount: permission denied (are you root?)\nOperation not permitted", ClassSandboxViolation},
		{"busybox privileged op", 1, false, "mount: permission denied (are you root?)", ClassSandboxViolation},
		{"violation text on a successful run is ignored", 0, false, "Read-only file system", ClassOK},
	}
	for _, c := range cases {
		if got, _ := ClassifyExit(c.code, c.oom, c.stderr); got != c.want {
			t.Errorf("%s: got %s want %s", c.name, got, c.want)
		}
	}
}

func TestCreateArgsCarryEveryIsolationFlag(t *testing.T) {
	s := goodSpec()
	s.Stdin, s.Env = "in", map[string]string{"K": "v"}
	args := strings.Join(CreateArgs(s, "n", time.Unix(1000, 0)), " ")
	for _, want := range []string{
		"--network none", "--read-only", "--cap-drop ALL", "--security-opt no-new-privileges",
		"--memory 64m", "--memory-swap 64m", "--cpus 0.5", "--pids-limit 128", "--user 65534:65534",
		"--tmpfs /tmp:rw,noexec,nosuid,nodev", "scheduler.sandbox.deadline=1000", "-i", "-e K=v", "alpine:3.20 echo hi",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("missing %q in %s", want, args)
		}
	}
	if strings.Contains(args, "--privileged") || strings.Contains(args, "--network host") {
		t.Fatal("must never be privileged or on the host network")
	}
}

// fakeDocker is an in-memory docker CLI.
type fakeDocker struct {
	mu        sync.Mutex
	calls     [][]string
	harden    func(*inspectDoc) // tamper with the inspected hardening
	state     inspectDoc        // State returned after start
	startSide func(ctx context.Context, so, se io.Writer)
	listing   string
}

func (f *fakeDocker) exec(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, args ...string) (int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, args)
	f.mu.Unlock()
	switch args[0] {
	case "create":
		stdout.Write([]byte("cid\n"))
	case "inspect":
		var d inspectDoc
		one := int64(128)
		d.HostConfig.NetworkMode, d.HostConfig.ReadonlyRootfs = "none", true
		d.HostConfig.CapDrop = []string{"ALL"}
		d.HostConfig.Memory, d.HostConfig.MemorySwap = 64<<20, 64<<20
		d.HostConfig.NanoCpus, d.HostConfig.PidsLimit = 500000000, &one
		d.HostConfig.SecurityOpt = []string{"no-new-privileges"}
		d.State = f.state.State
		if f.harden != nil {
			f.harden(&d)
		}
		b, _ := json.Marshal([]inspectDoc{d})
		stdout.Write(b)
	case "start":
		if f.startSide != nil {
			f.startSide(ctx, stdout, stderr)
		}
	case "ps":
		stdout.Write([]byte(f.listing))
	}
	return 0, nil
}

func (f *fakeDocker) saw(verb string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c[0] == verb {
			return true
		}
	}
	return false
}

func runner(f *fakeDocker) *DockerRunner { return &DockerRunner{Exec: f.exec} }

func TestRunClassifiesExitsAndAlwaysRemovesTheContainer(t *testing.T) {
	f := &fakeDocker{}
	f.state.State.ExitCode = 3
	f.startSide = func(_ context.Context, so, se io.Writer) { so.Write([]byte("out")); se.Write([]byte("err")) }
	o, err := runner(f).Run(context.Background(), goodSpec())
	if err != nil || o.Class != ClassNonZeroExit || o.ExitCode != 3 || o.Stdout != "out" || o.Stderr != "err" || !o.Failed() {
		t.Fatalf("outcome=%+v err=%v", o, err)
	}
	if !f.saw("rm") {
		t.Fatal("container must be removed")
	}

	f = &fakeDocker{}
	f.state.State.ExitCode, f.state.State.OOMKilled = 137, true
	if o, _ := runner(f).Run(context.Background(), goodSpec()); o.Class != ClassOOMKilled {
		t.Fatalf("want oom_killed, got %+v", o)
	}

	f = &fakeDocker{}
	if o, _ := runner(f).Run(context.Background(), goodSpec()); o.Class != ClassOK || o.Failed() {
		t.Fatalf("want ok, got %+v", o)
	}
}

func TestRunKillsAndClassifiesATimeout(t *testing.T) {
	f := &fakeDocker{}
	f.startSide = func(ctx context.Context, _, _ io.Writer) { <-ctx.Done() } // docker client blocks until killed
	s := goodSpec()
	s.TimeoutS = 1
	began := time.Now()
	o, err := runner(f).Run(context.Background(), s)
	if err != nil || o.Class != ClassTimeout {
		t.Fatalf("outcome=%+v err=%v", o, err)
	}
	if time.Since(began) > 4*time.Second {
		t.Fatal("timeout not enforced")
	}
	if !f.saw("kill") || !f.saw("rm") {
		t.Fatal("a timed-out container must be killed and removed (killing the docker client alone does not stop it)")
	}
}

func TestRunRefusesWhenHardeningIsNotInEffect(t *testing.T) {
	tampers := map[string]func(*inspectDoc){
		"network":    func(d *inspectDoc) { d.HostConfig.NetworkMode = "bridge" },
		"rw rootfs":  func(d *inspectDoc) { d.HostConfig.ReadonlyRootfs = false },
		"caps":       func(d *inspectDoc) { d.HostConfig.CapDrop = nil },
		"privileged": func(d *inspectDoc) { d.HostConfig.Privileged = true },
		"memory":     func(d *inspectDoc) { d.HostConfig.Memory = 0 },
		"swap":       func(d *inspectDoc) { d.HostConfig.MemorySwap = -1 },
		"cpu":        func(d *inspectDoc) { d.HostConfig.NanoCpus = 0 },
		"pids":       func(d *inspectDoc) { d.HostConfig.PidsLimit = nil },
		"nnp":        func(d *inspectDoc) { d.HostConfig.SecurityOpt = nil },
	}
	for name, tamper := range tampers {
		f := &fakeDocker{harden: tamper}
		o, err := runner(f).Run(context.Background(), goodSpec())
		if err == nil || !strings.Contains(err.Error(), "refusing to run") {
			t.Errorf("%s: want a refusal, got outcome=%+v err=%v", name, o, err)
		}
		if f.saw("start") {
			t.Errorf("%s: the container must never be started", name)
		}
		if !f.saw("rm") {
			t.Errorf("%s: the created container must still be removed", name)
		}
	}
}

func TestRunReportsDockerOutageAsAnErrorNotAJobFailure(t *testing.T) {
	r := &DockerRunner{Exec: func(context.Context, io.Reader, io.Writer, io.Writer, ...string) (int, error) {
		return -1, errors.New("docker: executable file not found")
	}}
	if o, err := r.Run(context.Background(), goodSpec()); err == nil {
		t.Fatalf("want an infrastructure error, got outcome %+v", o)
	}
}

func TestRunCallerCancellationIsNotAVerdict(t *testing.T) {
	f := &fakeDocker{}
	ctx, cancel := context.WithCancel(context.Background())
	f.startSide = func(c context.Context, _, _ io.Writer) { cancel(); <-c.Done() }
	if o, err := runner(f).Run(ctx, goodSpec()); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got outcome=%+v err=%v", o, err)
	}
	if !f.saw("kill") {
		t.Fatal("container must be killed")
	}
}

func TestOutputIsCapped(t *testing.T) {
	f := &fakeDocker{}
	f.startSide = func(_ context.Context, so, _ io.Writer) { so.Write([]byte(strings.Repeat("x", 3*outputLimit))) }
	o, _ := runner(f).Run(context.Background(), goodSpec())
	if len(o.Stdout) > outputLimit+64 || !strings.Contains(o.Stdout, "more bytes dropped") {
		t.Fatalf("stdout len=%d", len(o.Stdout))
	}
}

func TestSweepRemovesOnlyExpiredContainers(t *testing.T) {
	now := time.Unix(2000, 0)
	f := &fakeDocker{listing: "aaa 1000\nbbb 3000\nccc notanumber\n"}
	r := &DockerRunner{Exec: f.exec, Now: func() time.Time { return now }}
	n, err := r.Sweep(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("removed=%d err=%v", n, err)
	}
	var rm []string
	for _, c := range f.calls {
		if c[0] == "rm" {
			rm = append(rm, c[len(c)-1])
		}
	}
	if len(rm) != 1 || rm[0] != "aaa" {
		t.Fatalf("rm calls = %v", rm)
	}
}
