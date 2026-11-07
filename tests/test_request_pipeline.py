# Offline regression tests; no Kafka/Spark services or source writes.
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import dashboard_backend as backend
from pipeline import common
# Load the CLI script explicitly, without adding pipeline/ to the global
# import path or leaving its standalone 'common' alias in sys.modules.
_spec = importlib.util.spec_from_file_location("replay_under_test", ROOT / "pipeline" / "replay_producer.py")
if _spec is None or _spec.loader is None:
    raise ImportError("Cannot load the replay producer for testing")
replay = importlib.util.module_from_spec(_spec)
with patch.dict(sys.modules, {"common": common}):
    _spec.loader.exec_module(replay)

class FakeFuture:
    is_done = False
    def __init__(self, producer, offset):
        self.producer, self.offset = producer, offset
        self.callbacks = []
    def add_callback(self, callback):
        self.callbacks.append(callback)
        return self
    def get(self, timeout):
        # Old synchronous send/get blocks here before multiple sends are queued.
        if self.producer.sent < 3:
            raise AssertionError("Producer still waits after every send")
        if self.producer.fail:
            raise TimeoutError("broker acknowledgement timed out")
        metadata = SimpleNamespace(partition=0, offset=self.offset)
        self.is_done = True
        for callback in self.callbacks:
            callback(metadata)
        return metadata

class FakeProducer:
    def __init__(self, fail=False):
        self.sent, self.fail = 0, fail
    def send(self, *args, **kwargs):
        self.sent += 1
        return FakeFuture(self, self.sent - 1)

    def flush(self, timeout):
        pass
    def close(self, timeout):
        pass
class RequestPipelineTests(unittest.TestCase):
    def run_replay(self, fail=False, args=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "events.csv"
            source.write_text("timestamp,visitorid,event,itemid,transactionid\n"
                              "1000,1,view,20,\n2000,1,addtocart,20,\n3000,1,transaction,20,9\n")
            state = root / "replay.json"
            producer = FakeProducer(fail)
            with patch.object(replay, "DATA", source), patch.object(replay, "ARTIFACTS", root), \
                 patch.object(replay, "REPLAY_STATE", state), \
                 patch.object(replay, "KafkaProducer", return_value=producer), \
                 patch.object(sys, "argv", ["replay", *(args or ["--new-count", "3", "--rate", "0"])]):
                if fail:
                    with self.assertRaises(TimeoutError):
                        replay.main()
                else:
                    replay.main()
            result = json.loads(state.read_text())
            trace = [json.loads(line) for line in (root / "capture-trace.jsonl").read_text().splitlines()]
            return result, trace
    def test_batched_sends_preserve_actual_acknowledgements(self):
        state, trace = self.run_replay()
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["published"], 3)
        self.assertEqual([row["kafka_offset"] for row in trace], [0, 1, 2])
        self.assertTrue(all(row["kafka_acknowledged_at"] for row in trace))

    def test_failed_ack_saves_terminal_error(self):
        state, trace = self.run_replay(fail=True)
        self.assertEqual(state["status"], "error")
        self.assertIn("timed out", state["error"])
        self.assertEqual(state["published"], 0)
        self.assertEqual(trace, [])
    def test_resumed_replay_preserves_original_capture_totals(self):
        state, trace = self.run_replay(args=[
            "--new-count", "3", "--rate", "0", "--capture-id", "capture-1",
            "--capture-published", "2", "--requested-total", "5",
        ])
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["requested"], 5)
        self.assertEqual(state["newly_published"], 5)
        self.assertEqual(state["attempt_published"], 3)
        self.assertEqual(state["capture_id"], "capture-1")
        self.assertTrue(all(row["capture_id"] == "capture-1" for row in trace))

    def test_producer_uses_idempotence_and_long_retriable_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "events.csv"
            source.write_text("timestamp,visitorid,event,itemid,transactionid\n"
                              "1000,1,view,20,\n2000,1,addtocart,20,\n"
                              "3000,1,transaction,20,9\n", encoding="utf-8")
            producer = FakeProducer()
            with patch.object(replay, "DATA", source), \
                 patch.object(replay, "ARTIFACTS", root), \
                 patch.object(replay, "REPLAY_STATE", root / "replay.json"), \
                 patch.object(replay, "KafkaProducer", return_value=producer) as producer_factory, \
                 patch.object(sys, "argv", ["replay", "--new-count", "3", "--rate", "0"]):
                replay.main()
            config = producer_factory.call_args.kwargs
            self.assertTrue(config["enable_idempotence"])
            self.assertEqual(config["acks"], "all")
            self.assertEqual(config["max_in_flight_requests_per_connection"], 1)
            self.assertEqual(config["delivery_timeout_ms"], 180000)
            self.assertEqual(config["retry_backoff_ms"], 1000)
            self.assertNotIn("retries", config)

    def test_fresh_spark_can_accept_first_request_without_prior_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "events.csv"
            source.write_text("timestamp,visitorid,event,itemid,transactionid\n", encoding="utf-8")
            with patch.object(backend, "ARTIFACTS", Path(directory)), \
                 patch.object(backend, "DATA", source), \
                 patch.object(backend, "kafka_status", return_value={"status": "reachable", "host": "localhost", "port": 9092}), \
                 patch.object(backend, "spark_status", return_value={"status": "running"}), \
                 patch.object(backend, "read_json", return_value={}), \
                 patch.object(backend, "dashboard_data"), \
                 patch.object(backend, "_capture_process", None), \
                 patch.object(backend.subprocess, "Popen", return_value=SimpleNamespace(pid=123)):
                result, code = backend.request_capture(3, 100)
            self.assertEqual(code, 202, result)

    def test_dead_producer_cannot_leave_running_status(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "replay.json"
            state.write_text(json.dumps({"status": "running", "published": 2}))
            with patch.object(backend, "REPLAY_STATE", state), \
                 patch.object(backend, "_capture_process", SimpleNamespace(poll=lambda: 1)):
                self.assertFalse(backend.capture_is_running())
            result = json.loads(state.read_text())
            self.assertEqual(result["status"], "error")
            self.assertEqual(result["published"], 2)
    def test_resume_uses_remaining_count_and_original_capture_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "events.csv"
            source.write_text("timestamp,visitorid,event,itemid,transactionid\n", encoding="utf-8")
            state = root / "replay.json"
            state.write_text(json.dumps({"status": "error", "published": 5, "requested": 10,
                                         "newly_published": 2, "capture_id": "capture-1"}))
            metrics = root / "metrics.json"
            metrics.write_text(json.dumps({"processed_events": 5}))
            process = SimpleNamespace(pid=123)
            with patch.object(backend, "ARTIFACTS", root), \
                 patch.object(backend, "DATA", source), \
                 patch.object(backend, "REPLAY_STATE", state), \
                 patch.object(backend, "METRICS", metrics), \
                 patch.object(backend, "kafka_status", return_value={"status": "reachable", "host": "localhost", "port": 9092}), \
                 patch.object(backend, "spark_status", return_value={"status": "running"}), \
                 patch.object(backend, "dashboard_data"), \
                 patch.object(backend, "_capture_process", None), \
                 patch.object(backend.subprocess, "Popen", return_value=process) as popen:
                result, code = backend.request_capture(8, 10, resume=True)
            self.assertEqual(code, 202, result)
            self.assertEqual(result["requested"], 10)
            self.assertEqual(result["remaining"], 8)
            command = popen.call_args.args[0]
            self.assertEqual(command[command.index("--new-count") + 1], "8")
            self.assertEqual(command[command.index("--capture-id") + 1], "capture-1")
            self.assertEqual(command[command.index("--capture-published") + 1], "2")
            self.assertEqual(command[command.index("--requested-total") + 1], "10")

    def test_completed_producer_with_pending_spark_keeps_polling(self):
        import ast
        tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8-sig"))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "pipeline_is_busy")
        states = {"replay": {"status": "completed", "published": 3},
                  "spark": {"status": "running", "active_batch_status": "idle"},
                  "metrics": {"processed_events": 1}}
        namespace = {"REPLAY_STATE": "replay", "SPARK_STATE": "spark", "METRICS": "metrics",
                     "read_json": lambda path, default: states[path]}
        exec(compile(ast.Module(body=[function], type_ignores=[]), "app.py", "exec"), namespace)
        self.assertTrue(namespace["pipeline_is_busy"]())
        states["metrics"] = {"processed_events": 1, "invalid_records": 1, "duplicates": 1}
        self.assertFalse(namespace["pipeline_is_busy"]())

if __name__ == "__main__":
    unittest.main()
