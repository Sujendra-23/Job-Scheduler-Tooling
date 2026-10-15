# API Gateway, Lambda, and Step Functions

This optional path runs independently of the RPC dispatcher. It reuses the
`internal/sqsqueue` naming convention, queue topology, and Go `queue.Job` JSON
contract, plus the inference implementation in `ml/inference_runner.py`.

```text
POST /jobs -> submit Lambda -> <prefix>-p1 ... <prefix>-p5
                                      |
                             SQS-triggered Lambda
                                      |
                          STANDARD Step Functions execution
                                      |
                           RunJob (inference Lambda)
                              |                  |
                          Completed         Retry (1s, 2s)
                                                 |
                                        <prefix>-dlq -> Failed
```

The deployment is described in `template.yaml`: API Gateway REST API, three
Lambda functions, five SQS event source mappings, scoped IAM roles, and a
STANDARD state machine. Existing queues remain outside the stack's ownership.

## Run on LocalStack

Requirements: Docker with Compose, Python 3.10+, and Go 1.22+ for existing
queue/dispatcher tests. The tested emulator is pinned to LocalStack **4.14.0**.
Lambda uses actual Python 3.12 runtime containers, requiring access to the
Docker socket and a first-run runtime image download. On systems with a
different Docker socket, set `DOCKER_SOCK` before starting Compose.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r serverless/requirements.txt
make localstack-up
make serverless-deploy
```

The deploy helper defaults to `http://localhost:4566`, stack `sched-serverless`,
and queue prefix `sched`. It prints `SubmitUrl`, `StateMachineArn`, function
names, and queue URLs. It only permits local LocalStack endpoints and uses
dummy credentials. `make serverless-deploy` creates any missing queues with
the existing defaults: 30-second visibility, three receives, shared DLQ.
To attach exclusively to existing queues, omit `--create-queues`:

```bash
python serverless/deploy.py --prefix sched --stack sched-serverless
```

The helper checks that existing tier queues have at least 30-second visibility
(six times the five-second SQS handoff Lambda timeout) and redrive to the
expected DLQ. It leaves their attributes unchanged. Re-running the helper
updates the stack and uploads a new content-addressed Lambda archive.

Use the printed `SubmitUrl`:

```bash
curl -i -X POST \
  -H 'Content-Type: application/json' \
  -d '{"id":"example-1","priority":5,"batch_size":2,"precision":"fp32","seed":42}' \
  http://localhost:4566/restapis/<ApiId>/local/_user_request_/jobs
```

A successful request returns **202**:

```json
{"id":"example-1","priority":5,"message_id":"...","status":"queued"}
```

`id` defaults to a UUID; priority defaults to 3, batch size to 1, precision to
fp32, and seed to 1. Validation rejects invalid types, priorities outside 1–5,
batch sizes outside 1–256, unsupported precision, and unknown fields with
**400**. Bodies larger than 16 KiB return **413**. SQS submission errors return
**503**. This API does not deduplicate separate POST requests: repeating a
request, including after an ambiguous transport failure, can enqueue it twice.

## Lifecycle and delivery guarantees

- **Queue routing:** one mapping per priority tier, consumed concurrently.
  Priority selects the queue; independent Lambda consumers do **not** provide
  the RPC dispatcher's highest-tier-first polling guarantee. Run this path and
  the RPC dispatcher against different prefixes when testing them concurrently,
  since consumers on the same queues compete for messages.
- **Durable handoff:** the SQS Lambda returns success only after
  `StartExecution` succeeds. A deterministic execution name hashes the SQS
  message ID. STANDARD workflow idempotency handles a lost response; a
  completed execution's `ExecutionAlreadyExists` response also acknowledges
  the redelivery. Separate messages with the same caller job ID are distinct.
  AWS retains execution-name uniqueness for 90 days.
- **Partial batch failure:** mappings enable `ReportBatchItemFailures`.
  Invalid messages or failed workflow starts stay on SQS, retry after visibility
  expiry, and follow the queues' native redrive policy into the DLQ.
- **Inference:** Step Functions owns the job after handoff; SQS can acknowledge
  it before inference finishes. `RunJob` invokes the existing dependency-free
  CPU reference MLP and records its actual backend, predictions, latency, and
  attempt. The default archive does not include PyTorch and does not run
  ResNet-50; the input shape/model contract remains compatible with the Go path.
- **Retries:** a transient/task/timeout failure receives two retries with
  exponential backoff (three total attempts). `PermanentJobError` skips retry.
  This stack fixes the attempt budget at three; the RPC dispatcher's per-job
  triage and `MaxAttempts` handling are independent.
- **Terminal failure:** `DeadLetter` sends the original Go-compatible job body
  to the shared DLQ, with `Error`, `Cause`, and `ExecutionArn` message attributes,
  then terminates the execution as `FAILED`/`JobDeadLettered`. DLQ writes have
  their own bounded retries; if they exhaust, the workflow remains failed and
  its execution history retains the job/error for investigation. Native SQS
  redrive messages retain their original bodies and lack workflow attributes.
- **Inspection:** use Step Functions `DescribeExecution`/`GetExecutionHistory`
  for durable status/results and CloudWatch Lambda logs for `started` and
  `completed` records. This path does not write the RPC dispatcher's local
  JSONL file. The packaged inference task is deterministic and has no external
  side effects; any future task adding such effects must tolerate retries.

The LocalStack stack exposes an unauthenticated local API. The CloudFormation
template describes the AWS resources, but real AWS deployment, authentication,
load/scale behavior, and IAM enforcement have not been verified by these tests.

## Verify success, recovery, and dead letters

```bash
make serverless-test
make serverless-integration
SQS_ENDPOINT=http://localhost:4566 go test -race ./...
```

Unit tests cover validation, routing, Go-compatible message encoding,
partial batch failures, duplicate handoff, queue outages, and inference.
The opt-in integration suite creates isolated queues and a CloudFormation
stack and exercises real API Gateway HTTP requests, SQS event mappings,
Lambda runtime containers, and Step Functions. It checks repeat deployment
against existing queues without changing their settings, all five tiers,
successful inference, transient recovery on attempt two, poison-job exhaustion
at three attempts, immediate permanent-failure dead lettering, completed
execution redelivery, and native SQS redrive before workflow handoff. It
deletes its own stack, queues, and code bucket afterward. The last test briefly
reduces its isolated queue's visibility to five seconds to accelerate redrive.
GitHub Actions runs these checks in `.github/workflows/serverless.yml`.

Failure injection is disabled by default. To enable local demos:

```bash
python serverless/deploy.py --create-queues --allow-failure-injection
```

Add `"inject_failure":"transient"` (fails once), `"poison"` (exhausts retries),
or `"corrupt_input"` (permanent failure) to a request. These injection names
are also carried in the queue's `InjectFailure` field. The integration suite
enables injection only in its isolated stack.

`make localstack-down` stops the emulator and discards this Compose service's
ephemeral state. It does not delete resources in any remote AWS account.

References: [Lambda partial batch failures](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html),
[STANDARD execution idempotency](https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html),
[Step Functions error handling](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html),
and [LocalStack Lambda](https://docs.localstack.cloud/aws/services/lambda/).
