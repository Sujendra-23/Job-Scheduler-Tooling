// Package sandbox runs untrusted jobs inside Docker containers with hard
// resource limits and no way out: no network, a read-only root filesystem, all
// Linux capabilities dropped, no privilege escalation, a PID cap, and a
// wall-clock timeout enforced from outside the container.
//
// Every run ends in exactly one Class. Anything other than ClassOK is a job
// failure that the dispatcher sends to the dead-letter queue; an error from
// Runner.Run is different: it means the sandbox itself could not be trusted or
// reached (Docker down, hardening not applied), so the job did not run and
// must not be judged.
package sandbox

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Class is how a sandboxed run ended.
type Class string

const (
	ClassOK               Class = "ok"
	ClassNonZeroExit      Class = "non_zero_exit"
	ClassTimeout          Class = "timeout"
	ClassOOMKilled        Class = "oom_killed"
	ClassSandboxViolation Class = "sandbox_violation" // the job tried to break a boundary (network, read-only fs, privileged op, blocked syscall)
)

// Spec describes one sandboxed job. It is what travels inside queue.Job.
type Spec struct {
	Image     string            `json:"image"`
	Cmd       []string          `json:"cmd"`
	Stdin     string            `json:"stdin,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	CPUs      float64           `json:"cpus"`       // CPU quota, e.g. 0.5 = half a core
	MemoryMB  int               `json:"memory_mb"`  // hard limit; swap is disabled
	TimeoutS  int               `json:"timeout_s"`  // wall clock, includes container start
	PidsLimit int               `json:"pids_limit"` // 0 = DefaultPidsLimit
}

const (
	DefaultPidsLimit = 128
	MaxCPUs          = 16
	MaxMemoryMB      = 16384
	MaxTimeout       = time.Hour
)

var (
	imageRE  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/:@+-]*$`)
	envKeyRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func (s Spec) Timeout() time.Duration { return time.Duration(s.TimeoutS) * time.Second }

func (s Spec) pids() int {
	if s.PidsLimit > 0 {
		return s.PidsLimit
	}
	return DefaultPidsLimit
}

// Validate rejects specs that would run unbounded or that smuggle options into
// the docker command line.
func (s Spec) Validate() error {
	if !imageRE.MatchString(s.Image) {
		return fmt.Errorf("sandbox: invalid image %q", s.Image)
	}
	if len(s.Cmd) == 0 || s.Cmd[0] == "" {
		return fmt.Errorf("sandbox: cmd is required")
	}
	if s.CPUs <= 0 || s.CPUs > MaxCPUs {
		return fmt.Errorf("sandbox: cpus %v is outside (0, %d]", s.CPUs, MaxCPUs)
	}
	if s.MemoryMB < 6 || s.MemoryMB > MaxMemoryMB {
		return fmt.Errorf("sandbox: memory_mb %d is outside [6, %d]", s.MemoryMB, MaxMemoryMB)
	}
	if s.TimeoutS < 1 || s.Timeout() > MaxTimeout {
		return fmt.Errorf("sandbox: timeout_s %d is outside [1, %d]", s.TimeoutS, int(MaxTimeout.Seconds()))
	}
	if s.PidsLimit < 0 {
		return fmt.Errorf("sandbox: pids_limit must not be negative")
	}
	for k := range s.Env {
		if !envKeyRE.MatchString(k) {
			return fmt.Errorf("sandbox: invalid env name %q", k)
		}
	}
	return nil
}

// Outcome is the result of one run that reached a verdict.
type Outcome struct {
	Class    Class         `json:"class"`
	ExitCode int           `json:"exit_code"`
	Stdout   string        `json:"stdout,omitempty"` // capped
	Stderr   string        `json:"stderr,omitempty"` // capped
	Duration time.Duration `json:"duration"`
	Detail   string        `json:"detail,omitempty"`
}

// Failed reports whether the job must be dead-lettered.
func (o Outcome) Failed() bool { return o.Class != ClassOK }

// Runner executes a Spec. The error return is for infrastructure problems
// (see package doc); a job that ran and failed is an Outcome, not an error.
type Runner interface {
	Run(ctx context.Context, spec Spec) (Outcome, error)
}

// violationSignatures are stderr fragments that mean the job hit a sandbox
// boundary. This is heuristic: it labels attempts, it is not what contains them
// (the Docker flags do that).
var violationSignatures = []struct{ substr, what string }{
	{"Read-only file system", "tried to write to the read-only root filesystem"},
	{"Network is unreachable", "tried to use the network (disabled)"},
	{"Network unreachable", "tried to use the network (disabled)"},
	{"bad address", "tried to resolve a hostname (network disabled)"},
	{"Temporary failure in name resolution", "tried to resolve a hostname (network disabled)"},
	{"Name or service not known", "tried to resolve a hostname (network disabled)"},
	{"Operation not permitted", "attempted a privileged operation (capabilities dropped)"},
	{"are you root?", "attempted a privileged operation (runs as an unprivileged user)"},
	{"Bad system call", "used a blocked system call"},
}

// ExitSIGSYS is 128+SIGSYS(31): the process was killed for a blocked syscall.
const ExitSIGSYS = 159

// ClassifyExit turns a finished, non-timed-out run into a Class.
func ClassifyExit(exitCode int, oomKilled bool, stderr string) (Class, string) {
	switch {
	case oomKilled:
		return ClassOOMKilled, "killed by the kernel OOM killer at the memory limit"
	case exitCode == 0:
		return ClassOK, ""
	case exitCode == ExitSIGSYS:
		return ClassSandboxViolation, "used a blocked system call (SIGSYS)"
	}
	for _, sig := range violationSignatures {
		if strings.Contains(stderr, sig.substr) {
			return ClassSandboxViolation, sig.what
		}
	}
	if exitCode == 137 {
		return ClassNonZeroExit, "exit 137: SIGKILL from outside the job"
	}
	return ClassNonZeroExit, fmt.Sprintf("exited with status %d", exitCode)
}
