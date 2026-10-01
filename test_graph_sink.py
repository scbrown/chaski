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
        self.queries: list[str] = []
        self.extra: list[str] = []  # facts written by someone else (another vocabulary)
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
                    fake.queries.append(body["query"])
                    pats = re.findall(r"<([^>]+)> <([^>]+eventId)> \?e", body["query"])
                    rows = []
                    if fake.mode != "lose-after-write":
                        for ttl in list(fake.snapshots.values()) + fake.extra:
                            for subj, pred in pats:
                                if not ttl.startswith(f"<{subj}>"):
                                    continue
                                m = re.search("<" + re.escape(pred) + r"> (\"[^\"]*\")", ttl)
                                if m:
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
        self.assertEqual(em.deliver_pending(conn, sink, now=2.0, emitter=REACTION["label"]), (0, 1))
        q.mode = "ok"
        self.assertEqual(em.deliver_pending(conn, sink, now=10_000.0, emitter=REACTION["label"]), (1, 0))
        self.assertEqual(len(q.firings()), 1)
        q.server.shutdown()


class Load(unittest.TestCase):
    def test_the_reaction_write_takes_its_own_budget_slot(self):
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
        budget = em.WriteBudget(5.0)
        self.assertEqual(em.deliver_pending(conn, sink, 10.0, e.label, budget), (0, 0))
        self.assertEqual((q.knots, len(q.firings())), (1, 0), "slot 1: the Reaction only")
        self.assertEqual(em.deliver_pending(conn, sink, 12.0, e.label, budget), (0, 0))
        self.assertEqual(q.knots, 1, "no slot free 2 s later")
        self.assertEqual(em.deliver_pending(conn, sink, 15.0, e.label, budget), (1, 0))
        self.assertEqual((q.knots, len(q.firings())), (2, 1), "slot 2: the firing")
        q.server.shutdown()

    def test_the_sink_sends_its_own_client_label(self):
        self.assertEqual(gs.CLIENT_LABEL, "chaski-emitter")

    def test_a_backlog_drains_at_most_deliver_per_tick_per_tick(self):
        d = Path(tempfile.mkdtemp())
        out = d / "adapter.json"
        e = em.Emitter(label="x", owner="o", interval_s=10**9,
                       command=[sys.executable, "-c", "import sys; print(open(sys.argv[1]).read())", str(out)])
        conn = em.connect(d / "state.db")
        items = [f"i{n}" for n in range(12)]
        out.write_text(json.dumps([{"item": i, "verdict": "BLOCKED"} for i in items]))
        em.observe(conn, e, em.run_adapter(e), 1.0)
        out.write_text(json.dumps([{"item": i, "verdict": "UNBLOCKED", "event_id": f"id-{i}"} for i in items]))
        em.observe(conn, e, em.run_adapter(e), 2.0)
        sent = []

        class Count:
            def deliver(self, event):
                sent.append(event["event_id"])

        r = em.Runner(e, conn, Count())
        r.last_attempt = 3.0  # no adapter run on these ticks: delivery only
        for now in (4.0, 9.0, 14.0):
            before = len(sent)
            r.tick(now)
            self.assertLessEqual(len(sent) - before, em.DELIVER_PER_TICK)
        self.assertEqual(len(sent), 12)


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


class QuechuaDualRead(unittest.TestCase):
    """aegis-9dpcta: the read-back accepts eventId under the legacy AND the
    quechua term IRI. Instance IRIs (the firing) do not move."""

    def setUp(self):
        self.q = FakeQuipu()
        self.sink = gs.QuipuFiringSink(self.q.url, "t", REACTION, timeout=5)
        self.firing = gs.firing_iri(REACTION["label"], EVENT["event_id"])

    def tearDown(self):
        self.q.server.shutdown()

    def fact(self, term_ns):
        self.q.extra.append(f'<{self.firing}> <{term_ns}eventId> "{EVENT["event_id"]}" .\n')

    def test_an_event_id_under_either_term_iri_is_received(self):
        for ns in (gs.NS, gs.QUECHUA_NS):
            with self.subTest(ns=ns):
                self.q.extra.clear()
                self.fact(ns)
                self.assertTrue(self.sink._received(EVENT))

    def test_controls_absent_and_foreign_namespace_are_not_received(self):
        self.assertFalse(self.sink._received(EVENT))
        self.fact("http://example.org/other#")
        self.assertFalse(self.sink._received(EVENT))

    def test_the_read_back_is_one_request_a_union_and_no_join(self):
        self.fact(gs.QUECHUA_NS)
        self.sink._received(EVENT)
        [query] = self.q.queries
        self.assertIn(" UNION ", query)
        self.assertNotIn(" . ", query)
        self.assertIn(f"<{gs.QUECHUA_NS}eventId>", query)

    def test_instance_identity_stays_under_the_legacy_namespace(self):
        self.assertTrue(self.firing.startswith(gs.NS + "firing-"))
        self.assertIn(f"<{gs.NS}eventId>", self.sink.firing_turtle(EVENT))  # writers unchanged
