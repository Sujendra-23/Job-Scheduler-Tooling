#!/usr/bin/env python3
"""Deploy the CloudFormation stack to LocalStack, attaching existing queues.

Only --create-queues provisions absent queues; existing queue attributes must
already support Lambda consumption. The template itself never owns the queues.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
from urllib.parse import urlparse
import zipfile

import boto3
from botocore.exceptions import ClientError
import yaml

ROOT = Path(__file__).resolve().parents[1]


def session(endpoint, region="us-east-1"):
    host = urlparse(endpoint).hostname or ""
    if host not in ("localhost", "127.0.0.1", "::1") and not host.endswith(
        ".localstack.cloud"
    ):
        raise ValueError("this helper only deploys to local LocalStack endpoints")
    return boto3.Session(
        aws_access_key_id="test", aws_secret_access_key="test", region_name=region
    )


def package_code():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(ROOT / "serverless/handlers.py", "handlers.py")
        archive.write(ROOT / "ml/inference_runner.py", "ml/inference_runner.py")
    return output.getvalue()


def open_queues(sqs, prefix, create=False):
    def url(name, attributes=None):
        try:
            return sqs.get_queue_url(QueueName=name)["QueueUrl"]
        except ClientError as exc:
            if not create or exc.response["Error"]["Code"] not in (
                "AWS.SimpleQueueService.NonExistentQueue",
                "QueueDoesNotExist",
            ):
                raise
            return sqs.create_queue(QueueName=name, Attributes=attributes or {})[
                "QueueUrl"
            ]

    dlq_url = url(f"{prefix}-dlq")
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    urls, arns = {}, []
    for tier in range(1, 6):
        queue_url = url(
            f"{prefix}-p{tier}",
            {
                "VisibilityTimeout": "30",
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 3}
                ),
            },
        )
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])[
            "Attributes"
        ]
        redrive = json.loads(attrs.get("RedrivePolicy", "{}"))
        if (
            int(attrs["VisibilityTimeout"]) < 30
            or redrive.get("deadLetterTargetArn") != dlq_arn
        ):
            raise ValueError(
                f"{prefix}-p{tier} needs visibility >=30s and redrive to {prefix}-dlq"
            )
        urls[str(tier)] = queue_url
        arns.append(attrs["QueueArn"])
    return urls, arns, dlq_url, dlq_arn


def deploy(endpoint, stack, prefix, create_queues=False, failure_injection=False):
    aws = session(endpoint)
    sqs = aws.client("sqs", endpoint_url=endpoint)
    urls, arns, dlq_url, dlq_arn = open_queues(sqs, prefix, create_queues)
    s3 = aws.client("s3", endpoint_url=endpoint)
    bucket = f"{stack}-code"
    s3.create_bucket(Bucket=bucket)
    code = package_code()
    key = hashlib.sha256(code).hexdigest() + ".zip"
    s3.put_object(Bucket=bucket, Key=key, Body=code)
    cf = aws.client("cloudformation", endpoint_url=endpoint)
    template = yaml.safe_load((ROOT / "serverless/template.yaml").read_text())
    params = {
        "CodeBucket": bucket,
        "CodeKey": key,
        "QueueUrls": json.dumps(urls),
        **{f"Queue{tier}Arn": arn for tier, arn in enumerate(arns, 1)},
        "DLQUrl": dlq_url,
        "DLQArn": dlq_arn,
        "AllowFailureInjection": str(failure_injection).lower(),
    }
    args = {
        "StackName": stack,
        "TemplateBody": json.dumps(template),
        "Capabilities": ["CAPABILITY_IAM"],
        "Parameters": [
            {"ParameterKey": k, "ParameterValue": v} for k, v in params.items()
        ],
    }
    try:
        cf.describe_stacks(StackName=stack)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ValidationError":
            raise
        cf.create_stack(**args)
        waiter = "stack_create_complete"
    else:
        try:
            cf.update_stack(**args)
        except ClientError as exc:
            if "No updates are to be performed" not in str(exc):
                raise
            waiter = None
        else:
            waiter = "stack_update_complete"
    if waiter:
        try:
            cf.get_waiter(waiter).wait(
                StackName=stack, WaiterConfig={"Delay": 2, "MaxAttempts": 150}
            )
        except Exception:
            events = cf.describe_stack_events(StackName=stack)["StackEvents"]
            for event in events:
                if "FAILED" in event["ResourceStatus"]:
                    print(
                        event["LogicalResourceId"],
                        event.get("ResourceStatusReason", ""),
                    )
            raise
    outputs = {
        item["OutputKey"]: item["OutputValue"]
        for item in cf.describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
    }
    outputs.update(
        {
            "Endpoint": endpoint,
            "Stack": stack,
            "CodeBucket": bucket,
            "QueueUrls": urls,
            "DLQUrl": dlq_url,
            "SubmitUrl": f"{endpoint}/restapis/{outputs['ApiId']}/local/_user_request_/jobs",
        }
    )
    return outputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--endpoint", default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    )
    parser.add_argument("--stack", default="sched-serverless")
    parser.add_argument("--prefix", default="sched")
    parser.add_argument("--create-queues", action="store_true")
    parser.add_argument("--allow-failure-injection", action="store_true")
    args = parser.parse_args()
    outputs = deploy(
        args.endpoint,
        args.stack,
        args.prefix,
        args.create_queues,
        args.allow_failure_injection,
    )
    print(json.dumps(outputs, indent=2))


if __name__ == "__main__":
    main()
