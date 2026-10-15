import base64
import json
import os
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from serverless import handlers


class HandlersTest(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(
            os.environ,
            {
                "TIER_QUEUE_URLS": json.dumps(
                    {str(t): f"queue-{t}" for t in range(1, 6)}
                ),
                "STATE_MACHINE_ARN": "arn:aws:states:us-east-1:000000000000:stateMachine:test",
                "ALLOW_FAILURE_INJECTION": "false",
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.service = Mock()
        self.service.send_message.return_value = {"MessageId": "message-1"}
        self.clients = patch.object(handlers, "client", return_value=self.service)
        self.clients.start()
        self.addCleanup(self.clients.stop)

    def submit(self, data, **event):
        return handlers.submit({"body": json.dumps(data), **event}, None)

    def valid_job(self):
        self.submit({"id": "test", "priority": 5, "batch_size": 2})
        return json.loads(self.service.send_message.call_args.kwargs["MessageBody"])

    def test_routes_all_tiers_using_go_job_contract(self):
        for tier in range(1, 6):
            with self.subTest(tier=tier):
                result = self.submit(
                    {
                        "id": "job",
                        "priority": tier,
                        "batch_size": 2,
                        "precision": "fp16",
                        "seed": 42,
                    }
                )
                self.assertEqual(result["statusCode"], 202)
                args = self.service.send_message.call_args.kwargs
                self.assertEqual(args["QueueUrl"], f"queue-{tier}")
                job = json.loads(args["MessageBody"])
                self.assertEqual(
                    (job["ID"], job["Priority"], job["MaxAttempts"]), ("job", tier, 3)
                )
                self.assertEqual(job["Inference"]["seed"], 42)
                self.assertIn("SubmitTime", job)

    def test_validation_prevents_enqueue(self):
        invalid = [
            None,
            [],
            {"priority": 0},
            {"priority": 6},
            {"priority": True},
            {"batch_size": 0},
            {"batch_size": 257},
            {"batch_size": "2"},
            {"precision": "bad"},
            {"seed": 2**63},
            {"id": ""},
            {"id": 5},
            {"unknown": 1},
            {"inject_failure": "transient"},
        ]
        for request in invalid:
            with self.subTest(request=request):
                self.assertEqual(self.submit(request)["statusCode"], 400)
        self.service.send_message.assert_not_called()

    def test_bad_json_base64_and_size(self):
        self.assertEqual(handlers.submit({"body": "{"}, None)["statusCode"], 400)
        for body in ("{", "null", "[1]", "!!!"):
            with self.subTest(body=body):
                self.assertEqual(
                    handlers.submit({"body": body, "isBase64Encoded": True}, None)[
                        "statusCode"
                    ],
                    400,
                )
        self.assertEqual(
            handlers.submit({"body": "x" * 16385}, None)["statusCode"], 413
        )
        encoded = base64.b64encode(b'{"priority":2}').decode()
        self.assertEqual(
            handlers.submit({"body": encoded, "isBase64Encoded": True}, None)[
                "statusCode"
            ],
            202,
        )

    def test_queue_outage_does_not_return_accepted(self):
        self.service.send_message.side_effect = RuntimeError("offline")
        with self.assertLogs(handlers.LOG, level="ERROR"):
            self.assertEqual(self.submit({})["statusCode"], 503)

    def test_handoff_partial_failure_and_deterministic_execution_identity(self):
        job = self.valid_job()
        records = [
            {"messageId": "good", "body": json.dumps(job)},
            {"messageId": "bad", "body": "{"},
            {"messageId": "outage", "body": json.dumps(job)},
        ]
        self.service.start_execution.side_effect = [{}, RuntimeError("offline")]
        with self.assertLogs(handlers.LOG, level="ERROR"):
            result = handlers.start_jobs({"Records": records}, None)
        self.assertEqual(
            result,
            {
                "batchItemFailures": [
                    {"itemIdentifier": "bad"},
                    {"itemIdentifier": "outage"},
                ]
            },
        )
        name = self.service.start_execution.call_args_list[0].kwargs["name"]
        self.assertEqual(name, handlers.execution_name("good"))
        self.assertNotEqual(name, handlers.execution_name("outage"))
        self.assertLessEqual(len(name), 80)

    def test_completed_execution_redelivery_is_acknowledged(self):
        job = self.valid_job()
        self.service.start_execution.side_effect = ClientError(
            {"Error": {"Code": "ExecutionAlreadyExists"}}, "StartExecution"
        )
        self.assertEqual(
            handlers.start_jobs(
                {"Records": [{"messageId": "same", "body": json.dumps(job)}]}, None
            ),
            {"batchItemFailures": []},
        )

    def test_inference_runs_existing_reference_model(self):
        result = handlers.run_job({"job": self.valid_job(), "retry_count": 0}, None)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["result"]["backend"], "cpu-reference-mlp")
        self.assertEqual(len(result["result"]["top1_class_ids"]), 2)

    def test_transient_recovers_and_poison_and_corruption_fail(self):
        os.environ["ALLOW_FAILURE_INJECTION"] = "true"
        job = self.valid_job()
        job["InjectFailure"] = "transient"
        with self.assertRaises(handlers.TransientJobError):
            handlers.run_job({"job": job, "retry_count": 0}, None)
        self.assertEqual(
            handlers.run_job({"job": job, "retry_count": 1}, None)["attempt"], 2
        )
        job["InjectFailure"] = "poison"
        with self.assertRaises(handlers.TransientJobError):
            handlers.run_job({"job": job, "retry_count": 2}, None)
        job["InjectFailure"] = "corrupt_input"
        with self.assertRaises(handlers.PermanentJobError):
            handlers.run_job({"job": job, "retry_count": 0}, None)


if __name__ == "__main__":
    unittest.main()
