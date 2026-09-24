"""chaski tests, against a real HTTP fake of quipu's public API (not mocks), so
the request path chaski actually uses is exercised."""
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import chaski


class FakeQuipu:
    """/query returns `focus` rows (or fails with `status`); /events serves a log."""

    def __init__(self):
        self.focus: list[str] = []
        self.status = 200
        self.events: list[dict] = []
        self.requests: list[str] = []
        fake = self

        class H(BaseHTTPRequestHandler):
            def _send(self, code, obj):
                body = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):  # noqa: N802
                fake.requests.append("POST " + self.path)
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.path != "/query":
                    return self._send(404, {"error": "chaski must not call " + self.path})
                if fake.status != 200:
                    return self._send(fake.status, {"error": "query timed out"})
                self._send(200, {"rows": [{"focus": f} for f in fake.focus]})

            def do_GET(self):  # noqa: N802
                fake.requests.append("GET " + self.path)
                from urllib.parse import parse_qs, urlparse
                q = parse_qs(urlparse(self.path).query)
                since = int(q.get("since", ["0"])[0])
                limit = int(q.get("limit", ["500"])[0])
                types = set(q.get("types", [""])[0].split(","))
                page = [e for e in fake.events if e["offset"] > since and e["type"] in types][:limit]
                head = max((e["offset"] for e in fake.events), default=0)
                nxt = page[-1]["offset"] if page else since
                self._send(200, {"events": page, "next_offset": nxt, "lag": max(0, head - nxt)})

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()


def rule(**kw):
    base = dict(label="r", triggerKind="schedule", schedule="PT1M",
                condition="SELECT ?focus WHERE { ?focus a <urn:x> }",
                severity="warning", action="alert", owner="ops")
    base.update(kw)
    return chaski.Rule.from_dict(base)


class RuleValidation(unittest.TestCase):
    def test_fields_mirror_the_reaction_shape_and_unknown_fields_are_refused(self):
        with self.assertRaisesRegex(chaski.RuleError, "unknown field"):
            rule(expires="soon")

    def test_bad_enums_and_a_condition_without_focus_are_refused(self):
        for kw, msg in [({"severity": "loud"}, "severity"), ({"action": "email"}, "action"),
                        ({"triggerKind": "cron"}, "triggerKind"),
                        ({"condition": "SELECT ?x WHERE { ?x ?p ?o }"}, r"\?focus")]:
            with self.subTest(kw=kw), self.assertRaisesRegex(chaski.RuleError, msg):
                rule(**kw)

    def test_schedule_parses_and_an_event_rule_needs_types(self):
        self.assertEqual(rule(schedule="PT1H30M").interval_s, 5400)
        with self.assertRaises(chaski.RuleError):
            rule(schedule="every 5 minutes")
        with self.assertRaisesRegex(chaski.RuleError, "eventTypes"):
            rule(triggerKind="event", schedule="")

    def test_the_example_rules_file_loads(self):
        rules = chaski.load_rules(Path(__file__).with_name("rules.example.yaml"))
        self.assertIn("chaski-canary", [r.label for r in rules])


class Evaluation(unittest.TestCase):
    def setUp(self):
        self.q = FakeQuipu()
        self.tmp = tempfile.TemporaryDirectory()
        self.r = rule()
        self.reactor = chaski.Reactor(chaski.Quipu(self.q.url), [self.r],
                                      Path(self.tmp.name) / "state.json", 60)

    def tearDown(self):
        self.q.close()
        self.tmp.cleanup()

    def st(self):
        return self.reactor.states["r"]

    def test_firing_then_resolving_counts_transitions(self):
        self.q.focus = ["urn:a", "urn:b"]
        self.reactor.evaluate(self.r, 1.0)
        self.assertEqual(self.st().firing, {"urn:a", "urn:b"})
        self.q.focus = ["urn:b"]
        self.reactor.evaluate(self.r, 2.0)
        self.assertEqual(self.st().firing, {"urn:b"})
        self.assertEqual(self.st().transitions, 3)  # a, b started; a resolved

    def test_a_failed_query_is_UNKNOWN_and_KEEPS_the_firing_set(self):
        # The arm that matters: "could not look" must never read as "resolved".
        self.q.focus = ["urn:a"]
        self.reactor.evaluate(self.r, 1.0)
        self.q.status = 408
        self.reactor.evaluate(self.r, 2.0)
        self.assertEqual(self.st().firing, {"urn:a"})
        self.assertEqual(self.st().evaluations["unknown"], 1)
        self.assertEqual(self.st().last_success, 1.0)  # no fresh answer, so no fresh success
        self.assertIn('chaski_rule_firing{rule="r",focus="urn:a",severity="warning",keeper="ops"} 1',
                      self.reactor.metrics())

    def test_chaski_only_calls_public_read_endpoints(self):
        self.reactor.evaluate(self.r, 1.0)
        self.assertEqual(self.q.requests, ["POST /query"])

    def test_the_event_poll_floor_cannot_be_configured_away(self):
        r = chaski.Reactor(chaski.Quipu(self.q.url), [self.r], Path(self.tmp.name) / "s", 5)
        self.assertEqual(r.event_poll_s, chaski.MIN_EVENT_POLL_S)


class EventTail(unittest.TestCase):
    def setUp(self):
        self.q = FakeQuipu()
        self.tmp = tempfile.TemporaryDirectory()
        self.state = Path(self.tmp.name) / "state.json"
        self.r = rule(label="ev", triggerKind="event", schedule="", eventTypes="entity.added")
        self.q.events = [{"offset": i, "type": "entity.added"} for i in (1, 2, 3)]

    def tearDown(self):
        self.q.close()
        self.tmp.cleanup()

    def reactor(self):
        return chaski.Reactor(chaski.Quipu(self.q.url), [self.r], self.state, 60)

    def test_first_poll_starts_at_the_tail_and_does_not_replay_history(self):
        r = self.reactor()
        self.assertEqual(r.poll_events(0.0), set())
        self.assertEqual(r.cursor, 3)

    def test_a_new_matching_event_triggers_the_rule_and_the_cursor_survives_restart(self):
        r = self.reactor()
        r.poll_events(0.0)
        self.q.events.append({"offset": 4, "type": "entity.added"})
        self.assertEqual(r.poll_events(1.0), {"ev"})
        self.assertEqual(self.reactor().cursor, 4)  # a restarted chaski resumes exactly

    def test_a_non_matching_event_type_does_not_trigger(self):
        r = self.reactor()
        r.poll_events(0.0)
        self.q.events.append({"offset": 4, "type": "edge.retracted"})
        self.assertEqual(r.poll_events(1.0), set())

    def test_tick_evaluates_a_triggered_event_rule(self):
        r = self.reactor()
        r.tick(1000.0)  # first real tick: establishes the tail
        self.q.events.append({"offset": 4, "type": "entity.added"})
        self.q.focus = ["urn:z"]
        r.tick(1030.0)  # inside the poll floor: must NOT poll yet
        self.assertEqual(r.states["ev"].firing, set())
        r.tick(1061.0)
        self.assertEqual(r.states["ev"].firing, {"urn:z"})


if __name__ == "__main__":
    unittest.main()
