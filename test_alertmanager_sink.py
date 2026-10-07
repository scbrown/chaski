"""Alertmanager sink tests against a real HTTP fake of the v2 /alerts API, so
the request path the sink actually uses is exercised. The fake keys alerts on
their label set, the way Alertmanager does."""
import json
import tempfile
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import alertmanager_sink as am
import emitter as em

ALERT = {"alertname": "WorkItemLapsed", "severity": "warning",
         "summary": "{item} lapsed ({evidence})", "labels": {"team": "crew"}}
EVENT = {"event_id": "ev-1", "event": "due", "emitter": "entity-review-due", "item": "item-1",
         "generation": 1, "recipient": "owner-a", "evidence": "idle 9d > P7D", "observed_at": 1000.0}


class FakeAlertmanager:
    def __init__(self):
        self.alerts: dict[str, dict] = {}
        self.posts = 0
        self.mode = "ok"  # ok | refuse | drop
        self.auth_seen: list[str | None] = []
        fake = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                fake.auth_seen.append(self.headers.get("Authorization"))
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.posts += 1
                if fake.mode == "refuse":
                    self.send_response(400)
                    self.end_headers()
                    return
                if fake.mode != "drop":
                    for a in body:
                        fake.alerts[json.dumps(a["labels"], sort_keys=True)] = a
                self.send_response(200)
                self.end_headers()

            def do_GET(self):
                filters = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get("filter", [])
                want = dict(f.split("=", 1) for f in filters)
                want = {k: v.strip('"') for k, v in want.items()}
                hits = [a for a in fake.alerts.values()
                        if all(a["labels"].get(k) == v for k, v in want.items())]
                raw = json.dumps(hits).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"


class Delivery(unittest.TestCase):
    def setUp(self):
        self.f = FakeAlertmanager()
        self.sink = am.AlertmanagerSink(self.f.url, ALERT, "entity-review-due", clock=lambda: 5000.0)

    def tearDown(self):
        self.f.server.shutdown()

    def test_an_event_becomes_one_routed_alert(self):
        self.sink.deliver(EVENT)
        (a,) = self.f.alerts.values()
        self.assertEqual(a["labels"], {"alertname": "WorkItemLapsed", "severity": "warning", "team": "crew",
                                       "emitter": "entity-review-due", "event_id": "ev-1",
                                       "item": "item-1", "keeper": "owner-a"})
        self.assertEqual(a["annotations"]["summary"], "item-1 lapsed (idle 9d > P7D)")

    def test_a_resend_refreshes_the_same_alert_never_a_second(self):
        self.sink.deliver(EVENT)
        self.sink.deliver(EVENT)
        self.assertEqual((self.f.posts, len(self.f.alerts)), (2, 1))

    def test_a_late_send_still_opens_a_live_alert(self):
        # observed at t=1000 but sent at t=5000 after backoff: endsAt follows the SEND
        p = self.sink.payload(EVENT)
        self.assertEqual(p["startsAt"], am._iso(1000.0))
        self.assertEqual(p["endsAt"], am._iso(5000.0 + am.DEFAULT_TTL_S))

    def test_a_refused_post_raises_so_the_emitter_retries(self):
        self.f.mode = "refuse"
        with self.assertRaises(Exception):
            self.sink.deliver(EVENT)

    def test_an_accepted_alert_missing_on_read_back_is_not_received(self):
        self.f.mode = "drop"
        with self.assertRaises(ConnectionError):
            self.sink.deliver(EVENT)

    def test_a_recipient_that_is_not_a_name_routes_to_the_default_chain(self):
        p = self.sink.payload({**EVENT, "recipient": "two words; rm"})
        self.assertNotIn("keeper", p["labels"])
        p = self.sink.payload({**EVENT, "recipient": None})
        self.assertNotIn("keeper", p["labels"])

    def test_without_a_usable_recipient_the_emitter_owner_is_keeper(self):
        s = am.AlertmanagerSink(self.f.url, ALERT, "x", owner="owner-o")
        self.assertEqual(s.payload({**EVENT, "recipient": None})["labels"]["keeper"], "owner-o")
        self.assertEqual(s.payload({**EVENT, "recipient": "two words"})["labels"]["keeper"], "owner-o")
        self.assertEqual(s.payload(EVENT)["labels"]["keeper"], "owner-a", "a real recipient still wins")

    def test_basic_auth_is_sent_when_configured(self):
        s = am.AlertmanagerSink(self.f.url, ALERT, "x", user="u", password="p")
        s.deliver(EVENT)
        self.assertEqual(self.f.auth_seen[-1], "Basic dTpw")


class Chain(unittest.TestCase):
    def test_order_holds_and_a_later_failure_fails_the_whole_delivery(self):
        seen = []

        class Ok:
            def deliver(self, e):
                seen.append("graph")

        class Fail:
            def deliver(self, e):
                seen.append("am")
                raise ConnectionError("down")

        with self.assertRaises(ConnectionError):
            am.ChainSink([Ok(), Fail()]).deliver(EVENT)
        self.assertEqual(seen, ["graph", "am"])

    def test_the_outbox_row_stays_pending_until_every_sink_received(self):
        d = Path(tempfile.mkdtemp())
        conn = em.connect(d / "s.db")
        f = FakeAlertmanager()
        f.mode = "refuse"
        conn.execute("INSERT INTO outbox (event_id, emitter, item, generation, payload, created)"
                     " VALUES (?,?,?,?,?,?)", ("ev-1", "x", "item-1", 1, json.dumps(EVENT), 1.0))
        graph = []

        class G:
            def deliver(self, e):
                graph.append(e["event_id"])

        chain = am.ChainSink([G(), am.AlertmanagerSink(f.url, ALERT, "x")])
        self.assertEqual(em.deliver_pending(conn, chain, 10.0, "x"), (0, 1))
        f.mode = "ok"
        self.assertEqual(em.deliver_pending(conn, chain, 10.0 + 10**6, "x"), (1, 0))
        self.assertEqual((graph, len(f.alerts)), (["ev-1", "ev-1"], 1))
        f.server.shutdown()

    def test_setup_is_forwarded_to_the_sink_that_owes_it(self):
        class S:
            done = False

            def setup_pending(self):
                return not self.done

            def setup(self):
                self.done = True

            def deliver(self, e):
                pass

        s = S()
        c = am.ChainSink([s, am.AlertmanagerSink("http://unused.invalid", ALERT, "x")])
        self.assertTrue(c.setup_pending())
        c.setup()
        self.assertFalse(c.setup_pending())


class Wiring(unittest.TestCase):
    def _cfg(self, sink):
        d = Path(tempfile.mkdtemp())
        cfg = d / "e.yaml"
        cfg.write_text(json.dumps({"emitters": [{"label": "entity-review-due", "owner": "o",
                                                 "schedule": "PT1M", "command": ["true"], "sink": sink}]}))
        return d, cfg

    def test_alertmanager_beside_graph_builds_a_chain_graph_first(self):
        import chaski
        import graph_sink as gs

        d, cfg = self._cfg({})
        (d / "pw").write_text("p\n")
        graph = {"severity": "info", "action": ["alert"], "condition": "SELECT ?focus WHERE {}",
                 "comment": "c"}
        cfg.write_text(json.dumps({"emitters": [{
            "label": "entity-review-due", "owner": "o", "schedule": "PT1M", "command": ["true"],
            "sink": {"graph": graph, "alertmanager": {**ALERT, "url": "http://am.invalid",
                                                      "user": "u", "password_file": str(d / "pw")}}}]}))
        (e, sink), = chaski.load_emitters(cfg)
        built = chaski.make_sink(e, sink, "http://q")
        self.assertIsInstance(built, am.ChainSink)
        self.assertIsInstance(built.sinks[0], gs.QuipuFiringSink)
        self.assertEqual(built.sinks[1]._auth, "Basic dTpw")
        self.assertEqual(built.sinks[1].owner, "o")

    def test_alertmanager_alone_or_incomplete_is_refused(self):
        import chaski

        for sink in ({"alertmanager": {**ALERT, "url": "http://am.invalid"}},
                     {"graph": {"severity": "info", "action": ["alert"], "condition": "q", "comment": "c"},
                      "alertmanager": {"url": "http://am.invalid"}}):
            _, cfg = self._cfg(sink)
            with self.assertRaises(chaski.RuleError):
                chaski.load_emitters(cfg)


if __name__ == "__main__":
    unittest.main()
