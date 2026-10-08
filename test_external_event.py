import json
import tempfile
import unittest
from pathlib import Path

import emitter
from external_event import receive, identity
from chaski import load_emitters
from graph_sink import QuipuFiringSink


class ExternalEventControls(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.path = Path(self.root.name) / "emitter.db"
        self.conn = emitter.connect(self.path)
        self.addCleanup(self.root.cleanup)
        self.addCleanup(self.conn.close)
        self.definition = emitter.Emitter(label="release-pin", command=["/bin/false"],
                                         interval_s=86400, owner="kit", event="release.published",
                                         from_verdict="ABSENT", to_verdict="PUBLISHED",
                                         baseline_emits=True, externally_triggered=True)
        self.event = {"event_id": "github-release:example/kit:1", "type": "release.published",
                      "payload": {"tag": "v1.0.0"}}

    def outbox(self):
        return self.conn.execute("SELECT event_id,payload,delivered_at FROM outbox").fetchall()

    def test_first_published_and_restart_replay_reuse_existing_outbox(self):
        receipt = receive(self.conn, self.definition, self.event, 1)
        self.assertTrue(receipt["received"])
        self.assertEqual(len(self.outbox()), 1)
        frozen = self.outbox()[0]
        other = emitter.connect(self.path)
        try:
            self.assertEqual(receive(other, self.definition, self.event, 2), receipt)
            self.assertEqual(self.outbox(), [frozen])
        finally:
            other.close()

    def test_changed_payload_refuses_without_changing_frozen_delivery(self):
        receive(self.conn, self.definition, self.event, 1)
        frozen = self.outbox()
        self.event["payload"]["tag"] = "v2.0.0"
        with self.assertRaises(ValueError):
            receive(self.conn, self.definition, self.event, 2)
        self.assertEqual(self.outbox(), frozen)

    def test_external_ids_cannot_inject_graph_focus_and_emitters_have_distinct_ids(self):
        self.event["event_id"] = 'external > ; malicious " id'
        receive(self.conn, self.definition, self.event, 1)
        event_id, payload, _ = self.outbox()[0]
        self.assertEqual(json.loads(payload)["item"], event_id)
        self.assertRegex(event_id, r"^chaski-external-[0-9a-f]{64}$")
        self.assertNotEqual(identity("other", self.event["event_id"]), event_id)

    def test_missing_first_event_opt_in_is_a_refusal(self):
        self.definition.baseline_emits = False
        with self.assertRaises(ValueError):
            receive(self.conn, self.definition, self.event, 1)
        self.assertEqual(self.outbox(), [])

    def test_external_runner_delivers_without_invoking_an_adapter(self):
        receive(self.conn, self.definition, self.event, 1)
        delivered = []
        class Sink:
            def deliver(self, payload):
                delivered.append(payload)
        runner = emitter.Runner(self.definition, self.conn, Sink())
        runner.tick(100000)
        self.assertEqual(runner.runs, {"ok": 0, "unknown": 0})
        self.assertEqual(len(delivered), 1)
        self.assertIsNotNone(self.outbox()[0][2])
        receive(self.conn, self.definition, self.event, 100001)
        runner.tick(100002)
        self.assertEqual(len(delivered), 1)

    def test_failed_sink_retains_frozen_event_then_retries(self):
        receive(self.conn, self.definition, self.event, 1)
        attempts = []
        class Sink:
            def deliver(self, payload):
                attempts.append(payload)
                if len(attempts) == 1:
                    raise RuntimeError("held by operator")
        runner = emitter.Runner(self.definition, self.conn, Sink())
        runner.tick(10)
        self.assertIsNone(self.outbox()[0][2])
        runner.tick(1000)
        self.assertEqual(attempts[0], attempts[1])
        self.assertIsNotNone(self.outbox()[0][2])

    def test_loading_external_source_needs_no_adapter_and_rejects_false_opt_in(self):
        path = Path(self.root.name) / "emitters.yaml"
        for flag in ["false", "'true'", "true"]:
            path.write_text("emitters:\n  - label: release-pin\n    trigger: external\n"
                            "    schedule: PT24H\n    owner: kit\n    event: release.published\n"
                            f"    baseline_emits: {flag}\n    sink: {{jsonl: events.jsonl}}\n")
            if flag != "true":
                with self.assertRaises(ValueError):
                    load_emitters(path)
            else:
                configured = load_emitters(path)[0][0]
                self.assertTrue(configured.externally_triggered)
                self.assertEqual(configured.command, [])

    def test_graph_metadata_names_event_trigger_without_changing_schedule_default(self):
        reaction = {"label": "release-pin", "schedule": "PT24H", "owner": "kit", "severity": "info",
                    "condition": "authenticated published event", "action": ["alert"], "comment": "source"}
        sink = QuipuFiringSink("http://127.0.0.1", None, reaction)
        self.assertIn('triggerKind> "schedule"', sink.reaction_turtle())
        reaction.update(trigger_kind="event", event_types="release.published")
        self.assertIn('triggerKind> "event"', sink.reaction_turtle())
        self.assertIn('eventTypes> "release.published"', sink.reaction_turtle())


if __name__ == "__main__":
    unittest.main()
