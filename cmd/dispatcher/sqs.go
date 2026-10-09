package main

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"scheduler/internal/dispatch"
	"scheduler/internal/inference"
	"scheduler/internal/queue"
	"scheduler/internal/sandbox"
	"scheduler/internal/sqsqueue"
	"scheduler/internal/telemetry"
	"scheduler/internal/triage"
)

// rpcControl lets a worker process call the coordinator's SQSControl service.
type rpcControl struct{ c *rpc.Client }

func (r rpcControl) Started(a dispatch.StartedArgs, reply *dispatch.StartedReply) error {
	return r.c.Call("SQSControl.Started", a, reply)
}
func (r rpcControl) Report(a dispatch.SQSReportArgs, reply *dispatch.SQSReportReply) error {
	return r.c.Call("SQSControl.Report", a, reply)
}
func (r rpcControl) Status(a dispatch.StatusArgs, reply *dispatch.StatusReply) error {
	return r.c.Call("SQSControl.Status", a, reply)
}

// runSQSWorker is the worker process body when -queue-backend=sqs: it pulls
// batches straight from SQS and only uses the coordinator RPC for telemetry,
// triage and shutdown.
func runSQSWorker(brokerAddr, workerID, python, runnerScript, backend, sandboxMode string, cfg sqsqueue.Config) error {
	client, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("dial coordinator: %w", err)
	}
	defer client.Close()
	qs, err := sqsqueue.Open(context.Background(), cfg)
	if err != nil {
		return err
	}
	runner, err := startInferenceRunner(python, runnerScript, backend)
	if err != nil {
		return fmt.Errorf("start inference runner: %w", err)
	}
	defer runner.Close()

	run := func(job *queue.Job, attempt int) (inference.Result, error) {
		// Injected inference failures are consumed by the first attempt only.
		if attempt == 1 {
			if e := injectedError(job.InjectFailure); e != nil {
				return inference.Result{}, e
			}
		}
		return runner.Run(job.Inference)
	}
	opts := dispatch.WorkerOptions{Logf: func(format string, args ...any) { fmt.Printf(format+"\n", args...) }}
	switch sandboxMode {
	case "":
	case "docker":
		docker := &sandbox.DockerRunner{}
		// A previous worker killed mid-job leaves its container running; reap expired ones.
		if n, err := docker.Sweep(context.Background()); err == nil && n > 0 {
			fmt.Printf("%s: removed %d expired sandbox container(s)\n", workerID, n)
		}
		opts.Sandbox = docker
	default:
		return fmt.Errorf("unknown -sandbox %q (want docker)", sandboxMode)
	}
	return dispatch.RunSQSWorkerWith(context.Background(), qs, rpcControl{client}, workerID, run, opts)
}

// sandboxDemoJob returns the i-th job of a fixed mix that exercises every way a
// sandboxed run can end: success, non-zero exit, timeout, OOM kill, and two
// boundary violations (network, read-only filesystem).
func sandboxDemoJob(i int) *sandbox.Spec {
	image := "alpine:3.20"
	sh := func(script string, timeout int) *sandbox.Spec {
		return &sandbox.Spec{Image: image, Cmd: []string{"sh", "-c", script}, CPUs: 0.5, MemoryMB: 64, TimeoutS: timeout}
	}
	switch i % 6 {
	case 0:
		return sh("echo ok", 20)
	case 1:
		return sh("echo failing >&2; exit 3", 20)
	case 2:
		return sh("sleep 120", 3)
	case 3:
		return sh("tail /dev/zero", 20)
	case 4:
		return sh("wget -T 3 -q -O- http://1.1.1.1/", 20)
	default:
		return sh("echo x > /etc/x", 20)
	}
}

type sqsCoordinatorOpts struct {
	jobs, workers          int
	seed                   int64
	failureRate            float64
	backend, python        string
	runner, out            string
	agent                  triage.Agent
	recorder               *telemetry.Recorder
	chaosHang, chaosPoison int
	sandboxDemo            int
	keepQueues             bool
	sqs                    sqsqueue.Config
}

type workerProc struct {
	cmd    *exec.Cmd
	killed bool
	exited bool
}

func runSQSCoordinator(o sqsCoordinatorOpts) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	qs, err := sqsqueue.Create(ctx, o.sqs)
	if err != nil {
		return fmt.Errorf("create SQS queues (is LocalStack up at %q?): %w", o.sqs.Endpoint, err)
	}
	if !o.keepQueues {
		defer qs.Delete(context.Background())
	}
	coord := dispatch.NewSQSCoordinator(o.agent, o.recorder, qs)

	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("SQSControl", coord); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go rpcServer.ServeConn(conn)
		}
	}()

	jobs, err := generateJobs(o.jobs, o.seed, o.failureRate)
	if err != nil {
		return err
	}
	// Chaos jobs: the first N become poison pills, the next M "stall once".
	for i := range jobs {
		switch {
		case i < o.chaosPoison:
			jobs[i].InjectFailure = dispatch.InjectPoison
		case i < o.chaosPoison+o.chaosHang:
			jobs[i].InjectFailure = dispatch.InjectHang
		}
	}

	// Sandbox demo: the first N jobs become untrusted Docker-sandboxed jobs.
	for i := 0; i < o.sandboxDemo && i < len(jobs); i++ {
		jobs[i].Sandbox, jobs[i].InjectFailure = sandboxDemoJob(i), ""
	}

	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable, _ = filepath.Abs(executable)

	var (
		mu      sync.Mutex
		procs   = map[string]*workerProc{}
		nextID  = 0
		exits   = make(chan string, 1024)
		spawned = 0
	)
	spawn := func() error {
		mu.Lock()
		defer mu.Unlock()
		nextID++
		id := fmt.Sprintf("worker-%02d", nextID)
		cmd := exec.Command(executable, "worker", "-broker", listener.Addr().String(), "-worker-id", id,
			"-python", o.python, "-runner", o.runner, "-backend", o.backend,
			"-queue-backend", "sqs", "-sqs-endpoint", o.sqs.Endpoint, "-sqs-region", o.sqs.Region, "-sqs-prefix", o.sqs.Prefix)
		if o.sandboxDemo > 0 {
			cmd.Args = append(cmd.Args, "-sandbox", "docker")
		}
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start %s: %w", id, err)
		}
		p := &workerProc{cmd: cmd}
		procs[id] = p
		spawned++
		go func() {
			_ = cmd.Wait()
			mu.Lock()
			p.exited = true
			mu.Unlock()
			exits <- id
		}()
		return nil
	}

	// Chaos supervisor: when a worker takes a stalling batch, SIGKILL that OS
	// process (a hard crash: no cleanup, no ack) and start a replacement, so the
	// pool size stays constant and SQS has to redeliver the batch.
	coord.OnStarted = func(a dispatch.StartedArgs) {
		j := a.Job
		if !(j.InjectFailure == dispatch.InjectPoison || (j.InjectFailure == dispatch.InjectHang && a.Attempt == 1)) {
			return
		}
		go func() {
			time.Sleep(300 * time.Millisecond)
			mu.Lock()
			p := procs[a.WorkerID]
			if p != nil && !p.killed {
				p.killed = true
				_ = p.cmd.Process.Signal(syscall.SIGKILL)
				fmt.Printf("chaos: SIGKILL %s while it held %s (receive_count=%d)\n", a.WorkerID, j.ID, a.ReceiveCount)
			}
			mu.Unlock()
			_ = spawn()
		}()
	}

	for _, job := range jobs {
		if err := coord.Submit(ctx, job); err != nil {
			return err
		}
	}
	coord.CloseSubmissions()
	go coord.WatchDeadLetters(ctx, 500*time.Millisecond)

	for i := 0; i < o.workers; i++ {
		if err := spawn(); err != nil {
			return err
		}
	}

	alive := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, p := range procs {
			if !p.exited {
				n++
			}
		}
		return n
	}
	var runErr error
loop:
	for {
		select {
		case <-coord.Done():
			break loop
		case id := <-exits:
			mu.Lock()
			p := procs[id]
			killed := p.killed
			mu.Unlock()
			if killed {
				continue // expected chaos kill; replacement already spawned
			}
			select {
			case <-coord.Done():
				break loop
			default:
			}
			// An unexpected exit while work remains: other workers may still
			// finish it, but if none are left the run can never complete.
			time.Sleep(200 * time.Millisecond)
			if alive() == 0 {
				runErr = fmt.Errorf("all workers exited before every job reached a terminal state (last: %s)", id)
				break loop
			}
		}
	}

	// Let workers observe Done and exit on their own, then reap stragglers.
	deadline := time.After(10 * time.Second)
	for {
		if alive() == 0 {
			break
		}
		select {
		case <-exits:
		case <-deadline:
			mu.Lock()
			for _, p := range procs {
				if !p.exited {
					_ = p.cmd.Process.Kill()
				}
			}
			mu.Unlock()
			deadline = time.After(5 * time.Second)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if runErr != nil {
		return runErr
	}

	s := coord.Stats()
	fmt.Println("=== distributed inference complete (queue-backend=sqs) ===")
	fmt.Printf("worker processes spawned=%d submitted=%d succeeded=%d escalated=%d dead_lettered=%d retried=%d adjusted=%d redelivered=%d duplicates=%d\n",
		spawned, s.Submitted, s.Succeeded, s.Escalated, s.DeadLettered, s.Retried, s.Adjusted, s.Redelivered, s.Duplicates)
	fmt.Printf("event log written to %s\n", o.out)
	return nil
}
