"""Graph sink tests against a real HTTP fake of quipu's /knot and /query, so the
request path the sink actually uses is exercised. The fake keeps /knot
snapshots the way quipu does: replace_snapshot rewrites under the same key."""
import json
import re
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import emitter as em
import graph_sink as gs

REACTION = {"label": "workitem-unblocked", "schedule": "PT15M",
            "condition": "SELECT ?focus WHERE { ?focus <http://aegis.gastown.local/ontology/blockedOn> ?b }",
            "severity": "info", "action": ["alert"], "owner": "wu",
            "comment": "Candidates are WorkItems with blockedOn; the camayoc adapter decides the verdict."}


class FakeQuipu:
    def __init__(self):
        self.snapshots: dict[str, str] = {}
        self.knots = 0
        self.mode = "ok"  # ok | empty | refuse | lose-after-write
        fake = self

        class H(BaseHTTPRequestHandler):
            def _send(self, code, obj):
                body = b"" if obj is None else json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):  # noqa: N802
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                if self.path == "/knot":
                    fake.knots += 1
                    if fake.mode == "refuse":
                        return self._send(200, {"conforms": False, "violations": ["x"]})
                    fake.snapshots[body["snapshot"]] = body["turtle"]
                    if fake.mode == "empty":
                        return self._send(200, None)  # landed, but the answer was lost
                    return self._send(200, {"conforms": True, "tx_id": fake.knots})
                if self.path == "/query":
                    subj = re.search(r"<([^>]+)> <[^>]+eventId>", body["query"]).group(1)
                    rows = []
                    if fake.mode != "lose-after-write":
                        for ttl in fake.snapshots.values():
                            if ttl.startswith(f"<{subj}>"):
                                m = re.search(r"eventId> (\"[^\"]*\")", ttl)
                                rows.append({"e": json.loads(m.group(1))})
                    return self._send(200, {"rows": rows})
                self._send(404, {"error": self.path})

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def firings(self):
        return {k: v for k, v in self.snapshots.items() if "ReactionFiring" in v}


EVENT = {"event_id": "sha256:aa", "event": "unblocked", "emitter": "workitem-unblocked",
         "item": "aegis-abc123", "generation": 1, "recipient": "grant",
         "evidence": "sha256:ev", "observed_at": 1790400000.9}


class Sink(unittest.TestCase):
    def setUp(self):
        self.q = FakeQuipu()
        self.sink = gs.QuipuFiringSink(self.q.url, "t", REACTION, timeout=5)

    def tearDown(self):
        self.q.server.shutdown()

    def test_identity_is_label_and_event_id_and_a_resend_is_one_firing(self):
        self.sink.deliver(EVENT)
        self.sink.deliver(EVENT)
        self.assertEqual(len(self.q.firings()), 1)
        self.assertIn(gs.firing_iri(REACTION["label"], "sha256:aa"), self.q.firings())
        # A different reaction receiving the same event id is a different firing.
        self.assertNotEqual(gs.firing_iri("other", "sha256:aa"), gs.firing_iri(REACTION["label"], "sha256:aa"))

    def test_started_at_is_the_frozen_observed_at_and_focus_is_an_iri(self):
        self.sink.deliver(EVENT)
        ttl = next(iter(self.q.firings().values()))
        self.assertIn('"2026-09-26T05:20:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>', ttl)
        ns = "http://aegis.gastown.local/ontology/"
        self.assertIn(f"<{ns}focus> <{ns}aegis-abc123>", ttl)

    def test_the_reaction_is_written_once_and_is_typed(self):
        self.sink.deliver(EVENT)
        self.sink.deliver({**EVENT, "event_id": "sha256:bb"})
        reactions = [v for k, v in self.q.snapshots.items() if k.startswith("chaski:reaction:")]
        self.assertEqual(len(reactions), 1)
        self.assertIn("<http://aegis.gastown.local/ontology/Reaction>", reactions[0])
        self.assertIn('"alert"', reactions[0])

    def test_an_empty_answer_is_indeterminate_not_success(self):
        self.q.mode = "empty"
        with self.assertRaises(ConnectionError):
            self.sink.deliver(EVENT)

    def test_a_refusal_raises_and_names_it(self):
        self.q.mode = "refuse"
        with self.assertRaises(ValueError):
            self.sink.deliver(EVENT)

    def test_a_write_not_found_on_read_back_is_not_received(self):
        self.q.mode = "lose-after-write"
        with self.assertRaises(ConnectionError):
            self.sink.deliver(EVENT)

    def test_turtle_literals_are_escaped(self):
        r = {**REACTION, "comment": 'says "quoted" and \\ backslash'}
        ttl = gs.QuipuFiringSink(self.q.url, None, r).reaction_turtle()
        self.assertIn('"says \\"quoted\\" and \\\\ backslash"', ttl)


class ThroughTheEmitter(unittest.TestCase):
    def test_sabotage_lost_answer_then_retry_gives_exactly_one_firing(self):
        # The write LANDS but its answer is lost; the emitter keeps the row and
        # retries the identical write; the graph holds one firing.
        q = FakeQuipu()
        d = Path(tempfile.mkdtemp())
        out = d / "adapter.json"
        e = em.Emitter(label=REACTION["label"], owner="wu", interval_s=0,
                       command=[sys.executable, "-c", "import sys; print(open(sys.argv[1]).read())", str(out)])
        conn = em.connect(d / "state.db")
        for recs in ([{"item": "aegis-abc123", "verdict": "BLOCKED"}],
                     [{"item": "aegis-abc123", "verdict": "UNBLOCKED", "event_id": "sha256:aa"}]):
            out.write_text(json.dumps(recs))
            em.observe(conn, e, em.run_adapter(e), 1.0)
        sink = gs.QuipuFiringSink(q.url, "t", REACTION, timeout=5)
        q.mode = "empty"
        self.assertEqual(em.deliver_pending(conn, sink, now=2.0), (0, 1))
        q.mode = "ok"
        self.assertEqual(em.deliver_pending(conn, sink, now=10_000.0), (1, 0))
        self.assertEqual(len(q.firings()), 1)
        q.server.shutdown()


class Wiring(unittest.TestCase):
    def test_a_graph_sink_is_built_from_config_with_its_token_from_a_file(self):
        import chaski

        d = Path(tempfile.mkdtemp())
        (d / "tok").write_text("secret\n")
        cfg = d / "e.yaml"
        cfg.write_text(json.dumps({"emitters": [{
            "label": "workitem-unblocked", "owner": "wu", "schedule": "PT15M", "command": ["true"],
            "sink": {"graph": {**{k: REACTION[k] for k in ("severity", "action", "condition", "comment")},
                               "token_file": str(d / "tok")}}}]}))
        (e, sink), = chaski.load_emitters(cfg)
        built = chaski.make_sink(e, sink, "http://q")
        self.assertIsInstance(built, gs.QuipuFiringSink)
        self.assertEqual((built.token, built.reaction["schedule"]), ("secret", "PT900S"))

    def test_a_graph_sink_without_reaction_fields_is_refused(self):
        import chaski

        d = Path(tempfile.mkdtemp())
        cfg = d / "e.yaml"
        cfg.write_text(json.dumps({"emitters": [{"label": "x", "owner": "o", "schedule": "PT1M",
                                                 "command": ["true"], "sink": {"graph": {}}}]}))
        with self.assertRaises(chaski.RuleError):
            chaski.load_emitters(cfg)


if __name__ == "__main__":
    unittest.main()
