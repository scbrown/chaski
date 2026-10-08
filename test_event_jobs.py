import json
import tempfile
import subprocess
import sys
import time
import unittest
from pathlib import Path

from event_jobs import EventJobs, configuration, execute


class EventJobControls(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.path = Path(self.root.name) / "jobs.db"
        self.queue = EventJobs(self.path)
        self.addCleanup(self.root.cleanup)
        self.addCleanup(self.queue.close)
        self.event = {"event_id": "release:kit:1", "type": "release.published",
                      "payload": {"tag": "v1.0.0"}}
        self.calls = []

    def complete(self, envelope):
        self.calls.append(envelope)
        return {"event_id": envelope["event_id"], "job": "pin", "outcome": "complete"}

    def test_first_published_event_is_not_discarded_as_a_baseline(self):
        self.assertTrue(self.queue.enqueue(self.event, ["pin"])["received"])
        self.assertEqual(self.queue.pending(), 1)
        self.assertEqual(self.queue.run_one({"pin": self.complete}, 1, enabled=True), "complete")
        self.assertEqual(self.calls, [self.event])
        self.assertEqual(self.queue.pending(), 0)

    def test_unarmed_and_hold_never_invoke_receiver(self):
        self.queue.enqueue(self.event, ["pin"])
        self.assertEqual(self.queue.run_one({"pin": self.complete}, 1), "held")
        hold = Path(self.root.name) / "hold"
        hold.touch()
        self.assertEqual(self.queue.run_one({"pin": self.complete}, 2, enabled=True, hold=hold), "held")
        self.assertEqual(self.calls, [])
        self.assertEqual(self.queue.pending(), 1)

    def test_restart_duplicate_and_completed_event_replay_preserve_one_job(self):
        self.queue.enqueue(self.event, ["pin"])
        other = EventJobs(self.path)
        try:
            other.enqueue(self.event, ["pin", "pin"])
            self.assertEqual(other.pending(), 1)
            self.assertEqual(other.run_one({"pin": self.complete}, 1, enabled=True), "complete")
            other.enqueue(self.event, ["pin"])
            self.assertEqual(other.run_one({"pin": self.complete}, 2, enabled=True), "idle")
            self.assertEqual(len(self.calls), 1)
        finally:
            other.close()

    def test_same_identity_cannot_overwrite_payload(self):
        self.queue.enqueue(self.event, ["pin"])
        changed = json.loads(json.dumps(self.event))
        changed["payload"]["tag"] = "v2.0.0"
        with self.assertRaises(ValueError):
            self.queue.enqueue(changed, ["pin"])
        self.queue.run_one({"pin": self.complete}, 1, enabled=True)
        self.assertEqual(self.calls, [self.event])

    def test_wrong_or_unknown_receipt_cannot_complete_and_backoff_is_real(self):
        self.queue.enqueue(self.event, ["pin"])
        def wrong(envelope):
            self.calls.append(envelope)
            return {"event_id": "foreign", "job": "pin", "outcome": "complete"}
        self.assertEqual(self.queue.run_one({"pin": wrong}, 1, enabled=True), "unknown")
        self.assertEqual(self.queue.pending(), 1)
        self.assertEqual(self.queue.run_one({"pin": self.complete}, 2, enabled=True), "idle")
        self.assertEqual(self.queue.run_one({"pin": self.complete}, 6, enabled=True), "complete")
        self.assertEqual(self.calls, [self.event, self.event])

    def test_crash_after_external_effect_replays_identical_identity(self):
        self.queue.enqueue(self.event, ["pin"])
        effects = {self.event["event_id"]}  # receiver's durable side effect happened
        self.queue.db.execute("UPDATE event_jobs SET state='running',attempts=1")
        self.queue.db.commit()  # process crashed before saving its receipt
        def idempotent(envelope):
            effects.add(envelope["event_id"])
            return self.complete(envelope)
        self.assertEqual(self.queue.run_one({"pin": idempotent}, 1, enabled=True), "complete")
        self.assertEqual(effects, {self.event["event_id"]})
        self.assertEqual(self.calls, [self.event])

    def test_second_worker_cannot_run_while_receiver_is_in_flight(self):
        self.queue.enqueue(self.event, ["pin"])
        second = EventJobs(self.path)
        def exclusive(envelope):
            self.assertEqual(second.run_one({"pin": self.complete}, 2, enabled=True), "busy")
            return self.complete(envelope)
        try:
            self.assertEqual(self.queue.run_one({"pin": exclusive}, 1, enabled=True), "complete")
            self.assertEqual(self.calls, [self.event])
        finally:
            second.close()

    def test_missing_receiver_is_not_success(self):
        self.queue.enqueue(self.event, ["pin"])
        self.assertEqual(self.queue.run_one({}, 1, enabled=True), "unknown")
        self.assertEqual(self.queue.pending(), 1)

    def test_cli_enqueue_is_durable_while_disabled_and_executes_only_fixed_receiver(self):
        root = Path(self.root.name)
        receiver = root / "receiver.py"
        receiver.write_text("import sys,json\ne=json.load(sys.stdin)\n"
                            "print(json.dumps({'event_id':e['event_id'],'job':'pin','outcome':'complete'}))\n")
        config = root / "config.json"
        policy = {"enabled": False, "hold_file": str(root / "hold"), "jobs": {"pin": {
            "types": ["release.published"], "command": [sys.executable, str(receiver)], "timeout": 5}}}
        config.write_text(json.dumps(policy))
        command = [sys.executable, str(Path(__file__).with_name("event_jobs.py")),
                   "--state", str(self.path), "--config", str(config)]
        # Payload text cannot select argv or become a shell command.
        self.event["payload"]["command"] = ["sh", "-c", "exit 99"]
        queued = subprocess.run(command + ["enqueue"], input=json.dumps(self.event),
                                capture_output=True, text=True, check=True)
        self.assertTrue(json.loads(queued.stdout)["received"])
        held = subprocess.run(command + ["run-once"], capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(held.stdout)["outcome"], "held")
        self.assertEqual(self.queue.pending(), 1)
        policy["enabled"] = True
        config.write_text(json.dumps(policy))
        ran = subprocess.run(command + ["run-once"], capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(ran.stdout)["outcome"], "complete")
        self.assertEqual(self.queue.pending(), 0)

    def test_configuration_typos_and_ambiguous_enablement_refuse(self):
        path = Path(self.root.name) / "config.json"
        for value in [{"enable": True}, {"enabled": "true"}, {"enabled": True}]:
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                configuration(path)

    def test_receiver_timeout_kills_its_children_before_retry(self):
        marker = Path(self.root.name) / "escaped-child"
        child = f"import time,pathlib;time.sleep(1.7);pathlib.Path({str(marker)!r}).touch()"
        parent = "import subprocess,sys,time;subprocess.Popen([sys.executable,'-c'," + repr(child) + "]);time.sleep(10)"
        with self.assertRaisesRegex(RuntimeError, "timed out"):
            execute({"command": [sys.executable, "-c", parent], "timeout": 1}, self.event)
        time.sleep(1.2)
        self.assertFalse(marker.exists(), "timed-out descendant must not keep running")


if __name__ == "__main__":
    unittest.main()
