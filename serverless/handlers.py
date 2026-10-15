"""API submission, SQS-to-workflow handoff, and inference lifecycle task.

Queue bodies retain the Go queue.Job JSON contract. boto3 comes from the
Lambda runtime; the inference backend is packaged from ml/inference_runner.py.
"""

import base64
import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from functools import lru_cache

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from ml.inference_runner import build_backend

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


@lru_cache(maxsize=None)
def client(service):
    # LocalStack injects a container-reachable endpoint; never use host localhost
    # inside Lambda. With no endpoint, boto3 uses normal AWS credential discovery.
    return boto3.client(
        service,
        endpoint_url=os.getenv("AWS_ENDPOINT_URL") or None,
        config=Config(connect_timeout=1, read_timeout=2, retries={"max_attempts": 1}),
    )


def integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def validate_job(job):
    if not isinstance(job, dict):
        raise ValueError("job must be an object")
    if not isinstance(job.get("ID"), str) or not 1 <= len(job["ID"]) <= 128:
        raise ValueError("ID must contain 1 to 128 characters")
    integer(job.get("Priority"), "Priority", 1, 5)
    batch = job.get("Inference")
    if not isinstance(batch, dict):
        raise ValueError("Inference must be an object")
    integer(batch.get("batch_size"), "batch_size", 1, 256)
    integer(batch.get("seed"), "seed", -(2**63), 2**63 - 1)
    if batch.get("precision") not in ("fp32", "fp16"):
        raise ValueError("precision must be fp32 or fp16")
    if (
        batch.get("model"),
        batch.get("channels"),
        batch.get("height"),
        batch.get("width"),
    ) != ("resnet50", 3, 224, 224):
        raise ValueError("expected resnet50 with 3x224x224 inputs")
    injection = job.get("InjectFailure", "")
    if injection not in ("", "transient", "corrupt_input", "poison"):
        raise ValueError("unsupported failure injection")
    if injection and os.getenv("ALLOW_FAILURE_INJECTION") != "true":
        raise ValueError("failure injection is disabled")
    return job


def response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def submit(event, context):
    try:
        body = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body, validate=True).decode("utf-8")
        if len(body.encode("utf-8")) > 16384:
            return response(413, {"error": "request body exceeds 16 KiB"})
        request = json.loads(body)
        if not isinstance(request, dict):
            raise ValueError("request must be an object")
        unknown = set(request) - {
            "id",
            "priority",
            "batch_size",
            "precision",
            "seed",
            "inject_failure",
        }
        if unknown:
            raise ValueError(f"unknown fields: {', '.join(sorted(unknown))}")
        job = validate_job(
            {
                "ID": request.get("id", str(uuid.uuid4())),
                "Priority": request.get("priority", 3),
                "Inference": {
                    "model": "resnet50",
                    "batch_size": request.get("batch_size", 1),
                    "precision": request.get("precision", "fp32"),
                    "channels": 3,
                    "height": 224,
                    "width": 224,
                    "seed": request.get("seed", 1),
                },
                "Attempt": 0,
                "MaxAttempts": 3,
                "State": 0,
                "InjectFailure": request.get("inject_failure", ""),
                "SubmitTime": datetime.now(timezone.utc).isoformat(),
            }
        )
    except (ValueError, TypeError, UnicodeError) as exc:
        return response(400, {"error": str(exc)})
    try:
        queues = json.loads(os.environ["TIER_QUEUE_URLS"])
        sent = client("sqs").send_message(
            QueueUrl=queues[str(job["Priority"])], MessageBody=json.dumps(job)
        )
    except Exception:
        LOG.exception("queue submission failed")
        return response(503, {"error": "queue unavailable; retry submission"})
    return response(
        202,
        {
            "id": job["ID"],
            "priority": job["Priority"],
            "message_id": sent["MessageId"],
            "status": "queued",
        },
    )


def execution_name(message_id):
    # SQS message identity, rather than caller-supplied job ID, deduplicates
    # redelivery while allowing intentional submissions with the same job ID.
    return "sqs-" + hashlib.sha256(message_id.encode()).hexdigest()


def start_jobs(event, context):
    failures = []
    for record in event["Records"]:
        try:
            job = validate_job(json.loads(record["body"]))
            try:
                client("stepfunctions").start_execution(
                    stateMachineArn=os.environ["STATE_MACHINE_ARN"],
                    name=execution_name(record["messageId"]),
                    input=json.dumps(
                        {"job": job}, sort_keys=True, separators=(",", ":")
                    ),
                )
            except ClientError as exc:
                # Completed STANDARD executions reject redelivery too. A name
                # hashes one immutable SQS message, so this is the same work.
                if exc.response["Error"]["Code"] != "ExecutionAlreadyExists":
                    raise
        except Exception:
            LOG.exception("workflow handoff failed for %s", record["messageId"])
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


class TransientJobError(Exception):
    pass


class PermanentJobError(Exception):
    pass


@lru_cache(maxsize=1)
def backend():
    return build_backend(os.getenv("INFERENCE_BACKEND", "cpu-reference"))


def run_job(event, context):
    try:
        job = validate_job(event["job"])
    except (ValueError, KeyError, TypeError) as exc:
        raise PermanentJobError(str(exc)) from exc
    attempt = event["retry_count"] + 1
    LOG.info(json.dumps({"event": "started", "job_id": job["ID"], "attempt": attempt}))
    if job.get("InjectFailure") == "poison" or (
        job.get("InjectFailure") == "transient" and attempt == 1
    ):
        raise TransientJobError("injected transient failure")
    if job.get("InjectFailure") == "corrupt_input":
        raise PermanentJobError("injected corrupt input")
    result = backend().infer(job["Inference"])
    LOG.info(
        json.dumps(
            {
                "event": "completed",
                "job_id": job["ID"],
                "attempt": attempt,
                "result": result,
            }
        )
    )
    return {
        "status": "completed",
        "job_id": job["ID"],
        "attempt": attempt,
        "result": result,
    }
