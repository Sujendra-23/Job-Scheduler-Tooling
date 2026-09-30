// Command dispatcher runs real inference batches through multiple local OS
// worker processes. It is an honest local simulation of distributed dispatch:
// workers communicate with a coordinator over TCP RPC, but all processes run
// on one host unless launched separately with the worker subcommand.
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	_ "github.com/lib/pq"

	"scheduler/internal/dispatch"
	"scheduler/internal/inference"
	"scheduler/internal/queue"
	"scheduler/internal/resource"
	"scheduler/internal/telemetry"
	"scheduler/internal/triage"
)

type runnerResponse struct {
	OK     bool             `json:"ok"`
	Error  string           `json:"error"`
	Result inference.Result `json:"result"`
}

type inferenceRunner struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	encoder *json.Encoder
	decoder *json.Decoder
}

func startInferenceRunner(python, script, backend string) (*inferenceRunner, error) {
	cmd := exec.Command(python, script, "--backend", backend)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &inferenceRunner{cmd: cmd, stdin: stdin, encoder: json.NewEncoder(stdin), decoder: json.NewDecoder(bufio.NewReader(stdout))}, nil
}

func (r *inferenceRunner) Run(batch inference.Batch) (inference.Result, error) {
	if err := r.encoder.Encode(batch); err != nil {
		return inference.Result{}, err
	}
	var response runnerResponse
	if err := r.decoder.Decode(&response); err != nil {
		return inference.Result{}, err
	}
	if !response.OK {
		return inference.Result{}, errors.New(response.Error)
	}
	return response.Result, nil
}

func (r *inferenceRunner) Close() error {
	_ = r.stdin.Close()
	return r.cmd.Wait()
}

func injectedError(kind string) error {
	switch kind {
	case "oom":
		return errors.New("CUDA out of memory while allocating inference tensor")
	case "transient":
		return errors.New("transient GPU worker reset")
	case "corrupt_input":
		return errors.New("corrupt input image in batch")
	default:
		return nil
	}
}

func runWorker(args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	brokerAddress := fs.String("broker", "", "coordinator TCP address")
	workerID := fs.String("worker-id", "", "unique worker ID")
	python := fs.String("python", "python3", "Python interpreter")
	runnerScript := fs.String("runner", "ml/inference_runner.py", "inference runner path")
	backend := fs.String("backend", "auto", "auto, torch, or cpu-reference")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brokerAddress == "" || *workerID == "" {
		return errors.New("worker requires -broker and -worker-id")
	}

	client, err := rpc.Dial("tcp", *brokerAddress)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer client.Close()
	runner, err := startInferenceRunner(*python, *runnerScript, *backend)
	if err != nil {
		return fmt.Errorf("start inference runner: %w", err)
	}
	defer runner.Close()

	for {
		var claim dispatch.ClaimReply
		if err := client.Call("Broker.Claim", dispatch.ClaimArgs{WorkerID: *workerID}, &claim); err != nil {
			return fmt.Errorf("claim: %w", err)
		}
		if claim.Shutdown {
			return nil
		}
		if claim.Wait {
			time.Sleep(25 * time.Millisecond)
			continue
		}

		result, runErr := inference.Result{}, injectedError(claim.Job.InjectFailure)
		if runErr == nil {
			result, runErr = runner.Run(claim.Job.Inference)
		}
		report := dispatch.ReportArgs{WorkerID: *workerID, JobID: claim.Job.ID, Success: runErr == nil, Result: result}
		if runErr != nil {
			report.Error = runErr.Error()
		}
		var reply dispatch.ReportReply
		if err := client.Call("Broker.Report", report, &reply); err != nil {
			return fmt.Errorf("report %s: %w", claim.Job.ID, err)
		}
		if runErr != nil {
			fmt.Printf("%s: %s failed; triage=%s (%s)\n", *workerID, claim.Job.ID, reply.Decision.Action, reply.Decision.Reason)
		}
	}
}

func buildAgent(mode, url, key, model string) (triage.Agent, error) {
	policy := triage.PolicyAgent{}
	switch mode {
	case "policy":
		return policy, nil
	case "llm":
		if url == "" || model == "" {
			return nil, errors.New("-triage-mode=llm requires -triage-url and -triage-model")
		}
		return triage.FallbackAgent{
			Primary:  &triage.LLMAgent{URL: url, APIKey: key, Model: model},
			Fallback: policy,
		}, nil
	default:
		return nil, fmt.Errorf("unknown triage mode %q", mode)
	}
}

func runCoordinator(args []string) error {
	fs := flag.NewFlagSet("dispatcher", flag.ContinueOnError)
	numJobs := fs.Int("jobs", 12, "number of inference batches")
	numWorkers := fs.Int("workers", 3, "number of local worker processes")
	seed := fs.Int64("seed", 42, "random seed")
	failureRate := fs.Float64("failure-rate", 0.25, "fraction of first attempts with injected failures")
	out := fs.String("out", "scheduling_events.jsonl", "JSONL telemetry path")
	backend := fs.String("backend", "auto", "inference backend: auto, torch, or cpu-reference")
	python := fs.String("python", "python3", "Python interpreter")
	runnerScript := fs.String("runner", "ml/inference_runner.py", "inference runner path")
	triageMode := fs.String("triage-mode", "policy", "triage agent: policy or llm")
	triageURL := fs.String("triage-url", os.Getenv("TRIAGE_LLM_URL"), "OpenAI-compatible chat completions URL")
	triageModel := fs.String("triage-model", os.Getenv("TRIAGE_LLM_MODEL"), "LLM model name")
	triageKey := fs.String("triage-api-key", os.Getenv("TRIAGE_LLM_API_KEY"), "LLM API key (prefer environment variable)")
	pgDSN := fs.String("pg-dsn", "", "optional Postgres DSN")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *numJobs < 1 || *numWorkers < 1 {
		return errors.New("jobs and workers must be positive")
	}
	if *failureRate < 0 || *failureRate > 1 {
		return errors.New("failure-rate must be between 0 and 1")
	}
	if _, err := os.Stat(*runnerScript); err != nil {
		return fmt.Errorf("inference runner %s: %w", *runnerScript, err)
	}

	agent, err := buildAgent(*triageMode, *triageURL, *triageKey, *triageModel)
	if err != nil {
		return err
	}
	var db *sql.DB
	if *pgDSN != "" {
		db, err = sql.Open("postgres", *pgDSN)
		if err != nil {
			return err
		}
		if err := db.Ping(); err != nil {
			return err
		}
		defer db.Close()
	}
	recorder, err := telemetry.NewRecorder(*out, db)
	if err != nil {
		return err
	}
	defer recorder.Close()
	broker := dispatch.NewBroker(agent, recorder)

	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("Broker", broker); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go rpcServer.ServeConn(conn)
		}
	}()

	rng := rand.New(rand.NewSource(*seed))
	batchSizes := []int{1, 8, 16, 32}
	failureKinds := []string{"oom", "transient", "corrupt_input"}
	baseTime := time.Now()
	for i := 0; i < *numJobs; i++ {
		batchSize := batchSizes[rng.Intn(len(batchSizes))]
		precision := "fp32"
		if rng.Intn(2) == 0 {
			precision = "fp16"
		}
		batch, batchErr := inference.NewResNet50Batch(batchSize, precision, *seed+int64(i))
		if batchErr != nil {
			return batchErr
		}
		failure := ""
		if rng.Float64() < *failureRate {
			failure = failureKinds[i%len(failureKinds)]
		}
		job := &queue.Job{
			ID: fmt.Sprintf("inference-%03d", i), Priority: 1 + rng.Intn(5),
			Resources: resource.Requirements{CPUCores: 2, MemoryMB: 1024 + batchSize*64, GPUs: 1},
			Inference: batch, MaxAttempts: 3, InjectFailure: failure,
			SubmitTime: baseTime.Add(time.Duration(i) * time.Nanosecond),
		}
		if err := broker.Submit(job); err != nil {
			return err
		}
	}
	broker.CloseSubmissions()

	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable, _ = filepath.Abs(executable)
	var workers []*exec.Cmd
	for i := 0; i < *numWorkers; i++ {
		id := fmt.Sprintf("worker-%02d", i+1)
		cmd := exec.Command(executable, "worker", "-broker", listener.Addr().String(), "-worker-id", id,
			"-python", *python, "-runner", *runnerScript, "-backend", *backend)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			for _, started := range workers {
				_ = started.Process.Kill()
				_ = started.Wait()
			}
			return fmt.Errorf("start %s: %w", id, err)
		}
		workers = append(workers, cmd)
	}

	// Start reaping immediately. If a worker exits before the broker finishes
	// (for example, Python is missing), fail fast instead of leaving the
	// coordinator waiting forever on jobs that nobody can claim.
	workerExits := make(chan error, len(workers))
	for _, cmd := range workers {
		go func(cmd *exec.Cmd) { workerExits <- cmd.Wait() }(cmd)
	}
	received := 0
	var firstWorkerErr error
	for {
		select {
		case <-broker.Done():
			goto drainWorkers
		case workerErr := <-workerExits:
			received++
			select {
			case <-broker.Done():
				if workerErr != nil {
					firstWorkerErr = workerErr
				}
				goto drainWorkers
			default:
				for _, other := range workers {
					if other.Process != nil {
						_ = other.Process.Kill()
					}
				}
				for received < len(workers) {
					<-workerExits
					received++
				}
				if workerErr == nil {
					workerErr = errors.New("worker exited before all jobs reached a terminal state")
				}
				return fmt.Errorf("worker process: %w", workerErr)
			}
		}
	}

drainWorkers:
	for received < len(workers) {
		if workerErr := <-workerExits; workerErr != nil && firstWorkerErr == nil {
			firstWorkerErr = workerErr
		}
		received++
	}
	if firstWorkerErr != nil {
		return fmt.Errorf("worker process: %w", firstWorkerErr)
	}

	stats := broker.Stats()
	fmt.Println("=== distributed inference complete ===")
	fmt.Printf("workers=%d submitted=%d succeeded=%d escalated=%d retried=%d adjusted=%d\n",
		*numWorkers, stats.Submitted, stats.Succeeded, stats.Escalated, stats.Retried, stats.Adjusted)
	fmt.Printf("event log written to %s\n", *out)
	return nil
}

func main() {
	var err error
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		err = runWorker(os.Args[2:])
	} else {
		err = runCoordinator(os.Args[1:])
	}
	if err != nil {
		log.Printf("dispatcher: %v", err)
		os.Exit(1)
	}
}
