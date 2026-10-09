package sandbox

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Real-Docker tests. They skip when the docker daemon is not reachable or the
// image cannot be obtained, and can be forced off with SANDBOX_SKIP_DOCKER=1.
// SANDBOX_TEST_IMAGE overrides the image (default alpine:3.20).
func dockerRunnerOrSkip(t *testing.T) (*DockerRunner, string) {
	t.Helper()
	if os.Getenv("SANDBOX_SKIP_DOCKER") != "" {
		t.Skip("SANDBOX_SKIP_DOCKER set")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not installed")
	}
	if err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}").Run(); err != nil {
		t.Skip("docker daemon not reachable")
	}
	image := os.Getenv("SANDBOX_TEST_IMAGE")
	if image == "" {
		image = "alpine:3.20"
	}
	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		if out, err := exec.Command("docker", "pull", "-q", image).CombinedOutput(); err != nil {
			t.Skipf("image %s unavailable: %v: %s", image, err, out)
		}
	}
	return &DockerRunner{}, image
}

func sh(image, script string) Spec {
	return Spec{Image: image, Cmd: []string{"sh", "-c", script}, CPUs: 0.5, MemoryMB: 64, TimeoutS: 20}
}

func leftovers(t *testing.T) string {
	t.Helper()
	out, _ := exec.Command("docker", "ps", "-a", "-q", "--filter", "label="+labelSandbox+"=1").Output()
	return strings.TrimSpace(string(out))
}

func TestDockerSandboxRealContainers(t *testing.T) {
	r, image := dockerRunnerOrSkip(t)
	ctx := context.Background()
	if left := leftovers(t); left != "" {
		t.Logf("note: %d sandbox containers from earlier runs exist; checking only for new ones", len(strings.Fields(left)))
	}
	before := len(strings.Fields(leftovers(t)))

	t.Run("success captures stdout and runs as an unprivileged user", func(t *testing.T) {
		o, err := r.Run(ctx, sh(image, "echo hello; id -u"))
		if err != nil || o.Class != ClassOK || o.Stdout != "hello\n65534\n" {
			t.Fatalf("outcome=%+v err=%v", o, err)
		}
	})
	t.Run("stdin reaches the job", func(t *testing.T) {
		s := sh(image, "cat")
		s.Stdin = "from-stdin"
		if o, err := r.Run(ctx, s); err != nil || o.Class != ClassOK || o.Stdout != "from-stdin" {
			t.Fatalf("outcome=%+v err=%v", o, err)
		}
	})
	t.Run("non-zero exit", func(t *testing.T) {
		o, err := r.Run(ctx, sh(image, "echo nope >&2; exit 3"))
		if err != nil || o.Class != ClassNonZeroExit || o.ExitCode != 3 || !strings.Contains(o.Stderr, "nope") {
			t.Fatalf("outcome=%+v err=%v", o, err)
		}
	})
	t.Run("wall-clock timeout kills the container", func(t *testing.T) {
		s := sh(image, "sleep 60")
		s.TimeoutS = 2
		began := time.Now()
		o, err := r.Run(ctx, s)
		if err != nil || o.Class != ClassTimeout {
			t.Fatalf("outcome=%+v err=%v", o, err)
		}
		if d := time.Since(began); d > 15*time.Second {
			t.Fatalf("took %s", d)
		}
	})
	t.Run("memory limit produces an OOM kill", func(t *testing.T) {
		// busybox tail buffers an endless line in memory until the cgroup limit kills it.
		o, err := r.Run(ctx, sh(image, "tail /dev/zero"))
		if err != nil || o.Class != ClassOOMKilled {
			t.Fatalf("outcome=%+v err=%v", o, err)
		}
	})
	t.Run("no network", func(t *testing.T) {
		o, err := r.Run(ctx, sh(image, "wget -T 3 -q -O- http://1.1.1.1/"))
		if err != nil || o.Class != ClassSandboxViolation {
			t.Fatalf("outcome=%+v err=%v", o, err)
		}
	})
	t.Run("read-only root filesystem, writable tmpfs", func(t *testing.T) {
		o, err := r.Run(ctx, sh(image, "echo ok > /tmp/scratch && cat /tmp/scratch"))
		if err != nil || o.Class != ClassOK || o.Stdout != "ok\n" {
			t.Fatalf("tmpfs: outcome=%+v err=%v", o, err)
		}
		o, err = r.Run(ctx, sh(image, "echo x > /etc/pwned"))
		if err != nil || o.Class != ClassSandboxViolation {
			t.Fatalf("rootfs write: outcome=%+v err=%v", o, err)
		}
	})
	t.Run("dropped capabilities and no host mounts", func(t *testing.T) {
		o, err := r.Run(ctx, sh(image, "mount -t tmpfs none /mnt"))
		if err != nil || o.Class != ClassSandboxViolation {
			t.Fatalf("mount: outcome=%+v err=%v", o, err)
		}
		o, err = r.Run(ctx, sh(image, "ls /var/run/docker.sock"))
		if err != nil || o.Class != ClassNonZeroExit {
			t.Fatalf("docker.sock must not exist inside: outcome=%+v err=%v", o, err)
		}
	})
	t.Run("pid limit stops a fork bomb", func(t *testing.T) {
		s := sh(image, "i=0; while [ $i -lt 400 ]; do sleep 30 & i=$((i+1)); done; wait")
		s.PidsLimit, s.TimeoutS = 32, 10
		o, err := r.Run(ctx, s)
		if err != nil || !o.Failed() {
			t.Fatalf("a fork bomb must not succeed: outcome=%+v err=%v", o, err)
		}
	})
	t.Run("sweep removes an expired orphan", func(t *testing.T) {
		name := "sandbox-orphan-test"
		_ = exec.Command("docker", "rm", "-f", name).Run()
		if out, err := exec.Command("docker", "run", "-d", "--name", name, "--label", labelSandbox+"=1",
			"--label", labelDeadline+"=1", image, "sleep", "300").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		defer exec.Command("docker", "rm", "-f", name).Run()
		n, err := r.Sweep(ctx)
		if err != nil || n < 1 {
			t.Fatalf("swept=%d err=%v", n, err)
		}
		if err := exec.Command("docker", "inspect", name).Run(); err == nil {
			t.Fatal("orphan still exists")
		}
	})

	if after := len(strings.Fields(leftovers(t))); after > before {
		t.Fatalf("leaked %d sandbox containers", after-before)
	}
}
