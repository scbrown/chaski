"""Emitter tests. The adapter is a REAL subprocess reading a JSON file, so the
path the emitter actually uses (run the command, parse its stdout) is exercised.

Covers the cases the design names: first observation is a baseline, restart,
replayed close, UNKNOWN gap, initial UNBLOCKED baseline, a second genuine
transition, crash between send and checkpoint (the sabotage arm: emit, lose the
ack, re-run, exactly one event at the receiver), and crash inside observe.
"""
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import emitter as em

ADAPTER = "import json,sys; print(open(sys.argv[1]).read())"


def rec(item, verdict, event_id=None, evidence="e"):
    r = {"item": item, "verdict": verdict, "evidence": evidence, "assignee": "someone"}
    if event_id:
        r["event_id"] = event_id
    return r


class Harness:
    def __init__(self):
        self.dir = Path(tempfile.mkdtemp())
        self.out = self.dir / "adapter.json"
        self.db = self.dir / "state.db"
        self.emitter = em.Emitter(label="unblocked", owner="wu", interval_s=0,
                                  command=[sys.executable, "-c", ADAPTER, str(self.out)])
        self.conn = em.connect(self.db)
        self.sink = em.JsonlSink(self.dir / "received.jsonl")

    def run(self, records, now=1.0):
        self.out.write_text(json.dumps(records))
        return em.observe(self.conn, self.emitter, em.run_adapter(self.emitter), now)

    def restart(self):
        self.conn.close()
        self.conn = em.connect(self.db)

    def outbox(self):
        return self.conn.execute("SELECT event_id FROM outbox ORDER BY created").fetchall()


class Transitions(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def test_first_observation_is_a_baseline_never_an_event(self):
        self.assertEqual(self.h.run([rec("a", "BLOCKED"), rec("b", "UNBLOCKED", "id-b")]), [])
        self.assertEqual(self.h.outbox(), [])

    def test_blocked_to_unblocked_emits_exactly_once(self):
        self.h.run([rec("a", "BLOCKED")])
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1")]), ["id-a1"])
        # The same verdict again is steady state, not a new event.
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1")]), [])
        self.assertEqual(len(self.h.outbox()), 1)

    def test_restart_keeps_state_and_does_not_re_baseline(self):
        self.h.run([rec("a", "BLOCKED")])
        self.h.restart()
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1")]), ["id-a1"])

    def test_replayed_close_is_the_same_event(self):
        # Crash after the transition committed, then the same adapter answer:
        # INSERT OR IGNORE on the deterministic id keeps one row.
        self.h.run([rec("a", "BLOCKED")])
        self.h.run([rec("a", "UNBLOCKED", "id-a1")])
        self.h.conn.execute("UPDATE verdicts SET verdict='BLOCKED'")  # as if the state write were lost
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1")]), [])
        self.assertEqual(len(self.h.outbox()), 1)

    def test_unknown_gap_keeps_last_verdict_and_emits_once_when_evidence_returns(self):
        self.h.run([rec("a", "BLOCKED"), rec("b", "BLOCKED")])
        self.h.run([rec("a", "UNKNOWN"), rec("b", "UNKNOWN")])
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1"), rec("b", "BLOCKED")]), ["id-a1"])
        self.assertEqual(len(self.h.outbox()), 1)

    def test_unknown_first_sighting_is_not_a_baseline(self):
        self.h.run([rec("a", "UNKNOWN")])
        # Nothing known, so this UNBLOCKED is the baseline, not a transition.
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1")]), [])

    def test_a_second_genuine_transition_is_a_new_event(self):
        self.h.run([rec("a", "BLOCKED")])
        self.h.run([rec("a", "UNBLOCKED", "id-a1")])
        self.h.run([rec("a", "BLOCKED")])
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a2")]), ["id-a2"])
        gens = [g for (g,) in self.h.conn.execute("SELECT generation FROM outbox ORDER BY created")]
        self.assertEqual(gens, [1, 3])

    def test_absence_is_untracked_and_a_return_is_a_new_baseline(self):
        self.h.run([rec("a", "BLOCKED")])
        self.h.run([])  # closed, or its blockers were removed
        tracked = self.h.conn.execute("SELECT tracked FROM verdicts WHERE item='a'").fetchone()[0]
        self.assertEqual(tracked, 0)
        # We did not watch it unblock, so its return is not an event.
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1")]), [])

    def test_unblocked_without_event_id_refuses_the_whole_run(self):
        self.h.run([rec("a", "BLOCKED"), rec("b", "BLOCKED")])
        with self.assertRaises(em.ProtocolError):
            self.h.run([rec("a", "UNBLOCKED", "id-a1"), rec("b", "UNBLOCKED")])
        # All-or-nothing: a's valid transition did not land either.
        self.assertEqual(self.h.outbox(), [])
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1"), rec("b", "BLOCKED")]), ["id-a1"])

    def test_a_crash_inside_observe_leaves_nothing_and_the_rerun_emits_once(self):
        self.h.run([rec("a", "BLOCKED")])
        # Crash after the outbox insert but before COMMIT, via a trigger.
        self.h.conn.execute(
            "CREATE TEMP TRIGGER crash AFTER INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.h.run([rec("a", "UNBLOCKED", "id-a1")])
        self.h.conn.execute("DROP TRIGGER crash")
        self.assertEqual(self.h.outbox(), [])
        v = self.h.conn.execute("SELECT verdict FROM verdicts WHERE item='a'").fetchone()[0]
        self.assertEqual(v, "BLOCKED", "the verdict must roll back with the outbox row")
        self.assertEqual(self.h.run([rec("a", "UNBLOCKED", "id-a1")]), ["id-a1"])


class Delivery(unittest.TestCase):
    def setUp(self):
        self.h = Harness()
        self.h.run([rec("a", "BLOCKED")])
        self.h.run([rec("a", "UNBLOCKED", "id-a1")])

    def test_delivered_only_after_the_sink_returns(self):
        self.assertEqual(em.deliver_pending(self.h.conn, self.h.sink, now=2.0), (1, 0))
        self.assertEqual(self.h.sink.ids(), {"id-a1"})
        self.assertEqual(em.deliver_pending(self.h.conn, self.h.sink, now=3.0), (0, 0))

    def test_sabotage_send_then_lost_ack_then_rerun_gives_exactly_one_at_the_receiver(self):
        # The sink RECEIVES the event, then the sender dies before its checkpoint.
        class LostAck:
            def __init__(self, inner):
                self.inner = inner

            def deliver(self, event):
                self.inner.deliver(event)
                raise ConnectionError("killed before the ack")

        self.assertEqual(em.deliver_pending(self.h.conn, LostAck(self.h.sink), now=2.0), (0, 1))
        self.h.restart()
        # The retry re-sends the SAME id after backoff; the receiver dedupes.
        self.assertEqual(em.deliver_pending(self.h.conn, self.h.sink, now=10_000.0), (1, 0))
        lines = self.h.sink.path.read_text().splitlines()
        self.assertEqual(len(lines), 1, "exactly one event at the deduping receiver")

    def test_control_a_non_deduping_receiver_gets_the_duplicate(self):
        # Proves the arm above is not vacuous: the dedupe is what saved it.
        got = []

        class Naive:
            def deliver(self, event):
                got.append(event["event_id"])

        class LostAck:
            def deliver(self, event):
                got.append(event["event_id"])
                raise ConnectionError("killed before the ack")

        em.deliver_pending(self.h.conn, LostAck(), now=2.0)
        em.deliver_pending(self.h.conn, Naive(), now=10_000.0)
        self.assertEqual(got, ["id-a1", "id-a1"])

    def test_a_failing_sink_backs_off_and_keeps_the_row(self):
        class Down:
            def deliver(self, event):
                raise OSError("receiver down")

        self.assertEqual(em.deliver_pending(self.h.conn, Down(), now=2.0), (0, 1))
        attempts, nxt, err = self.h.conn.execute(
            "SELECT attempts, next_attempt, last_error FROM outbox").fetchone()
        self.assertEqual((attempts, nxt), (1, 2.0 + em.BASE_BACKOFF_S))
        self.assertIn("receiver down", err)
        # Not due yet: no attempt.
        self.assertEqual(em.deliver_pending(self.h.conn, Down(), now=3.0), (0, 0))


class RunnerTests(unittest.TestCase):
    def test_a_failed_adapter_run_changes_nothing(self):
        h = Harness()
        h.run([rec("a", "BLOCKED")])
        h.emitter.command = [sys.executable, "-c", "import sys; sys.exit(3)"]
        r = em.Runner(h.emitter, h.conn, h.sink)
        r.tick(5.0)
        self.assertEqual(r.runs, {"ok": 0, "unknown": 1})
        v, t = h.conn.execute("SELECT verdict, tracked FROM verdicts WHERE item='a'").fetchone()
        self.assertEqual((v, t), ("BLOCKED", 1), "a failed run must not untrack anything")

    def test_runner_emits_and_delivers_and_reports(self):
        h = Harness()
        r = em.Runner(h.emitter, h.conn, h.sink)
        h.out.write_text(json.dumps([rec("a", "BLOCKED")]))
        r.tick(1.0)
        h.out.write_text(json.dumps([rec("a", "UNBLOCKED", "id-a1")]))
        r.tick(2.0)
        self.assertEqual(h.sink.ids(), {"id-a1"})
        text = "\n".join(r.metrics(lambda s: s))
        self.assertIn('chaski_emitter_outbox_delivered_total{emitter="unblocked"} 1', text)
        self.assertIn('chaski_emitter_outbox_pending{emitter="unblocked"} 0', text)
        self.assertIn('chaski_emitter_runs_total{emitter="unblocked",result="ok"} 2', text)


if __name__ == "__main__":
    unittest.main()


class Wiring(unittest.TestCase):
    def test_emitters_yaml_loads_and_the_reactor_ticks_them(self):
        import chaski

        h = Harness()
        cfg = h.dir / "emitters.yaml"
        cfg.write_text(json.dumps({"emitters": [{
            "label": "unblocked", "owner": "wu", "schedule": "PT1M",
            "command": h.emitter.command, "sink": {"jsonl": str(h.sink.path)}}]}))
        (e, sink), = chaski.load_emitters(cfg)
        self.assertEqual((e.interval_s, sink["jsonl"]), (60, str(h.sink.path)))
        reactor = chaski.Reactor(chaski.Quipu("http://127.0.0.1:9"), [], h.dir / "cursor", 60)
        reactor.last_event_poll = float("inf")  # no event rules; keep the fake URL untouched
        reactor.runners = [em.Runner(e, h.conn, em.JsonlSink(sink["jsonl"]))]
        h.out.write_text(json.dumps([rec("a", "BLOCKED")]))
        reactor.tick(100.0)
        h.out.write_text(json.dumps([rec("a", "UNBLOCKED", "id-a1")]))
        reactor.tick(200.0)
        self.assertEqual(h.sink.ids(), {"id-a1"})
        self.assertIn("chaski_emitter_outbox_delivered_total", reactor.metrics())

    def test_an_emitter_without_a_sink_is_refused(self):
        import chaski

        h = Harness()
        cfg = h.dir / "emitters.yaml"
        cfg.write_text(json.dumps({"emitters": [{"label": "x", "owner": "o", "schedule": "PT1M",
                                                 "command": ["true"]}]}))
        with self.assertRaises(chaski.RuleError):
            chaski.load_emitters(cfg)


def due(entity, verdict, event_id=None):
    r = {"entity": entity, "verdict": verdict, "owner": "keeper", "evidence": "e"}
    if event_id:
        r["event_id"] = event_id
    return r


class DueEmitter(unittest.TestCase):
    """The review-age adapter (key 'entity', NOT_DUE -> DUE), with
    baseline_emits: an entity already overdue when first seen still reaches
    its owner."""

    def setUp(self):
        self.h = Harness()
        self.h.emitter.key, self.h.emitter.event = "entity", "due"
        self.h.emitter.from_verdict, self.h.emitter.to_verdict = "NOT_DUE", "DUE"
        self.h.emitter.baseline_emits = True

    def test_a_short_age_fires_once_when_it_lapses(self):
        self.assertEqual(self.h.run([due("claim", "NOT_DUE")]), [])
        self.assertEqual(self.h.run([due("claim", "DUE", "due-1")]), ["due-1"])
        self.assertEqual(self.h.run([due("claim", "DUE", "due-1")]), [], "one due event per expiry")

    def test_already_overdue_at_first_sight_is_an_event(self):
        self.assertEqual(self.h.run([due("claim", "DUE", "due-1")]), ["due-1"])
        payload = json.loads(self.h.conn.execute("SELECT payload FROM outbox").fetchone()[0])
        self.assertEqual((payload["event"], payload["recipient"]), ("due", "keeper"))

    def test_reverification_resets_the_clock_and_a_later_lapse_is_a_new_event(self):
        self.h.run([due("claim", "NOT_DUE")])
        self.h.run([due("claim", "DUE", "due-1")])
        self.assertEqual(self.h.run([due("claim", "NOT_DUE")]), [], "re-verified: no event")
        self.assertEqual(self.h.run([due("claim", "DUE", "due-2")]), ["due-2"])

    def test_control_an_entity_that_never_lapses_never_fires(self):
        for _ in range(3):
            self.assertEqual(self.h.run([due("claim", "NOT_DUE")]), [])
        self.assertEqual(self.h.outbox(), [])

    def test_the_blocked_vocabulary_is_refused_for_a_due_emitter(self):
        with self.assertRaises(em.ProtocolError):
            self.h.run([due("claim", "BLOCKED")])


class Retransition(unittest.TestCase):
    def test_same_verdict_with_a_new_event_id_is_a_new_event(self):
        # Re-blocked and unblocked again between two runs: the verdict reads
        # UNBLOCKED both times, only the adapter's id shows the second transition.
        h = Harness()
        h.run([rec("a", "BLOCKED")])
        h.run([rec("a", "UNBLOCKED", "id-a1")])
        self.assertEqual(h.run([rec("a", "UNBLOCKED", "id-a2")]), ["id-a2"])
        self.assertEqual(h.run([rec("a", "UNBLOCKED", "id-a2")]), [])

    def test_an_old_state_file_is_migrated_not_refused(self):
        h = Harness()
        h.conn.close()
        h.db.unlink()
        old = sqlite3.connect(str(h.db))
        old.executescript(em.SCHEMA.replace("    last_event TEXT,\n", ""))
        old.execute("INSERT INTO verdicts (emitter, item, verdict, generation, tracked, updated)"
                    " VALUES ('unblocked', 'a', 'BLOCKED', 0, 1, 0)")
        old.commit()
        old.close()
        h.conn = em.connect(h.db)
        self.assertEqual(h.run([rec("a", "UNBLOCKED", "id-a1")]), ["id-a1"])
