#!/usr/bin/env bash
# Demonstrates SQS redelivery + DLQ with REAL worker processes being SIGKILLed.
# Needs LocalStack with SQS on $SQS_ENDPOINT (default http://localhost:4566):
#   docker run -d --rm --name localstack-sqs -p 4566:4566 -e SERVICES=sqs localstack/localstack:3.8
# Scenario (visibility timeout 4s, maxReceiveCount 3, 3 workers, 8 jobs):
#   inference-000  "poison": kills every worker that receives it -> 3 deliveries -> DLQ
#   inference-001/002 "hang": first worker is SIGKILLed mid-job -> redelivered -> completes
#   the rest run normally (some with injected failures handled by triage).
set -euo pipefail
cd "$(dirname "$0")/.."
export SQS_ENDPOINT="${SQS_ENDPOINT:-http://localhost:4566}"
LOG="${1:-/tmp/sqs-demo.jsonl}"
make build >/dev/null
./bin/dispatcher -queue-backend sqs -sqs-prefix "demo$(date +%s)" -jobs 8 -workers 3 \
  -backend cpu-reference -failure-rate 0.5 -seed 7 -chaos-poison 1 -chaos-hang 2 \
  -sqs-visibility-timeout 4 -sqs-max-receive 3 -out "$LOG" | tee /tmp/sqs-demo.out
echo; echo "--- poison job: delivered 3x then dead-lettered ---"; ./bin/tracer -log "$LOG" -job inference-000
echo; echo "--- stalled-worker job: redelivered after the kill, then completed ---"; ./bin/tracer -log "$LOG" -job inference-001
grep -q 'dead_lettered=1' /tmp/sqs-demo.out && grep -q 'succeeded=7' /tmp/sqs-demo.out && grep -q 'redelivered=4' /tmp/sqs-demo.out \
  && echo "DEMO PASS" || { echo "DEMO FAIL"; exit 1; }
