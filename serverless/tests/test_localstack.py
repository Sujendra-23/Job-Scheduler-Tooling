"""Real HTTP -> SQS event mapping -> Lambda -> Step Functions integration.

Opt in with RUN_LOCALSTACK_TESTS=1. Every run gets isolated stack/queue names
and removes only its own resources, including when assertions fail.
"""

import json
import os
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid

from botocore.exceptions import ClientError

from serverless import deploy
from serverless.handlers import execution_name


def eventually(action, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = action()
        if value:
            return value
        time.sleep(1)
    raise AssertionError(f"condition did not become true within {timeout}s")


@unittest.skipUnless(
    os.getenv("RUN_LOCALSTACK_TESTS") == "1",
    "set RUN_LOCALSTACK_TESTS=1 for LocalStack integration",
)
class LocalStackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
        cls.stack = "sched-test-" + uuid.uuid4().hex[:12]
        cls.aws = deploy.session(cls.endpoint)
        cls.sqs = cls.aws.client("sqs", endpoint_url=cls.endpoint)
        cls.sfn = cls.aws.client("stepfunctions", endpoint_url=cls.endpoint)
        cls.lambdas = cls.aws.client("lambda", endpoint_url=cls.endpoint)
        cls.cf = cls.aws.client("cloudformation", endpoint_url=cls.endpoint)
        cls.addClassCleanup(cls.cleanup)
        cls.outputs = deploy.deploy(
            cls.endpoint,
            cls.stack,
            cls.stack,
            create_queues=True,
            failure_injection=True,
        )

    @classmethod
    def cleanup(cls):
        cls.cf.delete_stack(StackName=cls.stack)
        cls.cf.get_waiter("stack_delete_complete").wait(
            StackName=cls.stack, WaiterConfig={"Delay": 2, "MaxAttempts": 90}
        )
        # Queues are deliberately outside CloudFormation ownership.
        for name in [f"{cls.stack}-p{tier}" for tier in range(1, 6)] + [
            f"{cls.stack}-dlq"
        ]:
            try:
                queue = cls.sqs.get_queue_url(QueueName=name)["QueueUrl"]
                cls.sqs.delete_queue(QueueUrl=queue)
            except cls.sqs.exceptions.QueueDoesNotExist:
                pass
        s3 = cls.aws.resource("s3", endpoint_url=cls.endpoint)
        bucket = s3.Bucket(f"{cls.stack}-code")
        try:
            bucket.objects.all().delete()
            bucket.delete()
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchBucket":
                raise

    def post(self, data):
        req = Request(
            self.outputs["SubmitUrl"],
            data=json.dumps(data).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(req, timeout=40) as result:
                return result.status, json.load(result)
        except HTTPError as exc:
            with exc:
                return exc.code, json.load(exc)

    def execution(self, message_id):
        arn = (
            self.outputs["StateMachineArn"].replace(":stateMachine:", ":execution:")
            + ":"
            + execution_name(message_id)
        )

        def finished():
            try:
                result = self.sfn.describe_execution(executionArn=arn)
            except self.sfn.exceptions.ExecutionDoesNotExist:
                return None
            return result if result["status"] != "RUNNING" else None

        return eventually(finished)

    def attempts(self, execution):
        events = self.sfn.get_execution_history(executionArn=execution["executionArn"])[
            "events"
        ]
        return sum(
            e["type"] == "TaskStateEntered"
            and e["stateEnteredEventDetails"]["name"] == "RunJob"
            for e in events
        ), events

    def dead_letter(self, job_id):
        def find():
            messages = self.sqs.receive_message(
                QueueUrl=self.outputs["DLQUrl"],
                MaxNumberOfMessages=10,
                VisibilityTimeout=0,
                MessageAttributeNames=["All"],
            ).get("Messages", [])
            return next(
                (m for m in messages if json.loads(m["Body"]).get("ID") == job_id), None
            )

        return eventually(find)

    def test_00_redeployment_attaches_to_existing_queues(self):
        before = {
            tier: self.sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=["QueueArn", "RedrivePolicy", "VisibilityTimeout"],
            )["Attributes"]
            for tier, url in self.outputs["QueueUrls"].items()
        }
        again = deploy.deploy(
            self.endpoint, self.stack, self.stack, failure_injection=True
        )
        self.assertEqual(again["QueueUrls"], self.outputs["QueueUrls"])
        self.assertEqual(again["StateMachineArn"], self.outputs["StateMachineArn"])
        after = {
            tier: self.sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=["QueueArn", "RedrivePolicy", "VisibilityTimeout"],
            )["Attributes"]
            for tier, url in again["QueueUrls"].items()
        }
        self.assertEqual(before, after)

    def test_01_success_through_http_and_every_queue_tier(self):
        submitted = []
        for tier in range(1, 6):
            status, body = self.post(
                {"id": f"tier-{tier}", "priority": tier, "batch_size": 2}
            )
            self.assertEqual(status, 202, body)
            submitted.append((tier, body))
        for tier, body in submitted:
            execution = self.execution(body["message_id"])
            self.assertEqual(execution["status"], "SUCCEEDED", execution)
            self.assertEqual(json.loads(execution["input"])["job"]["Priority"], tier)
            output = json.loads(execution["output"])
            self.assertEqual(output["result"]["backend"], "cpu-reference-mlp")
            self.assertEqual(len(output["result"]["top1_class_ids"]), 2)
            self.assertEqual(output["attempt"], 1)

    def test_02_invalid_http_input_is_rejected(self):
        for data in ({"priority": 6}, {"batch_size": 0}, {"precision": "bad"}, []):
            with self.subTest(data=data):
                self.assertEqual(self.post(data)[0], 400)

    def test_03_transient_failure_retries_then_completes(self):
        status, body = self.post({"id": "transient", "inject_failure": "transient"})
        self.assertEqual(status, 202, body)
        execution = self.execution(body["message_id"])
        self.assertEqual(execution["status"], "SUCCEEDED", execution)
        self.assertEqual(json.loads(execution["output"])["attempt"], 2)

    def test_04_retry_exhaustion_dead_letters_original_job(self):
        status, body = self.post({"id": "poison", "inject_failure": "poison"})
        self.assertEqual(status, 202, body)
        execution = self.execution(body["message_id"])
        self.assertEqual(execution["status"], "FAILED", execution)
        self.assertEqual(execution["error"], "JobDeadLettered")
        message = self.dead_letter("poison")
        self.assertEqual(json.loads(message["Body"])["Inference"]["model"], "resnet50")
        self.assertEqual(
            message["MessageAttributes"]["Error"]["StringValue"], "TransientJobError"
        )
        _, events = self.attempts(execution)
        invocations = [
            e
            for e in events
            if e["type"] == "TaskScheduled"
            and e["taskScheduledEventDetails"]["resource"] == "invoke"
        ]
        self.assertEqual(len(invocations), 3, events)

    def test_05_permanent_failure_skips_retry(self):
        status, body = self.post({"id": "corrupt", "inject_failure": "corrupt_input"})
        self.assertEqual(status, 202, body)
        execution = self.execution(body["message_id"])
        self.assertEqual(execution["status"], "FAILED", execution)
        self.assertEqual(
            self.dead_letter("corrupt")["MessageAttributes"]["Error"]["StringValue"],
            "PermanentJobError",
        )
        _, events = self.attempts(execution)
        invocations = [
            e
            for e in events
            if e["type"] == "TaskScheduled"
            and e["taskScheduledEventDetails"]["resource"] == "invoke"
        ]
        self.assertEqual(len(invocations), 1, events)

    def test_06_completed_redelivery_starts_no_second_execution(self):
        status, body = self.post({"id": "redelivery"})
        self.assertEqual(status, 202, body)
        execution = self.execution(body["message_id"])
        event = {
            "Records": [
                {
                    "messageId": body["message_id"],
                    "body": json.dumps(json.loads(execution["input"])["job"]),
                }
            ]
        }
        invocation = self.lambdas.invoke(
            FunctionName=self.outputs["WorkerName"], Payload=json.dumps(event).encode()
        )
        self.assertNotIn("FunctionError", invocation)
        self.assertEqual(json.load(invocation["Payload"]), {"batchItemFailures": []})
        executions = self.sfn.list_executions(
            stateMachineArn=self.outputs["StateMachineArn"]
        )["executions"]
        self.assertEqual(
            sum(e["name"] == execution_name(body["message_id"]) for e in executions), 1
        )

    def test_07_invalid_queue_message_redrives_before_workflow_handoff(self):
        # Keep the worker's five-second minimum visibility while shortening this
        # isolated test's failure cycle (normal deployment keeps 30 seconds).
        queue = self.outputs["QueueUrls"]["1"]
        self.sqs.set_queue_attributes(
            QueueUrl=queue, Attributes={"VisibilityTimeout": "5"}
        )
        try:
            self.sqs.send_message(
                QueueUrl=queue, MessageBody=json.dumps({"ID": "invalid", "Priority": 1})
            )
            message = self.dead_letter("invalid")
            self.assertEqual(
                json.loads(message["Body"]), {"ID": "invalid", "Priority": 1}
            )
            self.assertNotIn("Error", message.get("MessageAttributes", {}))
        finally:
            self.sqs.set_queue_attributes(
                QueueUrl=queue, Attributes={"VisibilityTimeout": "30"}
            )


if __name__ == "__main__":
    unittest.main()
