"""Delivery invariants for changed subsets, restart recovery and time deadlines."""
import json
from urllib.parse import parse_qs, urlparse

import pytest

import incremental as inc
from emitter import Emitter, ProtocolError, connect, observe


class Sink:
    def deliver(self, event):
        pass


def record(item, verdict="BLOCKED"):
    return {"item": item, "verdict": verdict, "event_id": "event-" + item}


@pytest.fixture
def setup(tmp_path, monkeypatch):
    conn = connect(tmp_path / "state.db")
    calls = []
    def adapter(emitter, request=None):
        calls.append(request)
        if request is None:
            return {"version": 1, "attributes": ["status"], "graphs": ["ROOT"]}
        if "route" in request:
            return {"items": sorted({c["entity"] for c in request["route"]})}
        if request.get("discover"):
            return {"items": ["a", "b"]}
        scope = set(request["items"]) | {c["entity"] for c in request["changes"]}
        return {"scope": list(scope), "records": [record(i) for i in scope], "next_checks": {i: None for i in scope}}
    monkeypatch.setattr(inc, "call", adapter)
    e = Emitter("e", ["unused"], 86400, "owner", change_driven=True)
    runner = inc.ChangeRunner(e, conn, Sink())
    yield conn, e, runner, calls
    conn.close()


def change(tx=2, entity="a", graph="ROOT", attr="status"):
    return {"tx": tx, "sequence": 0, "entity": entity, "attribute": attr,
            "graph": graph, "op": "assert", "value": "closed"}


def test_partial_run_does_not_untrack_other_items(setup):
    conn, e, _, _ = setup
    observe(conn, e, [record("a"), record("b")], 1)
    observe(conn, e, [], 2, scope={"a"})
    assert dict(conn.execute("SELECT item,tracked FROM verdicts")) == {"a": 0, "b": 1}
    with pytest.raises(ProtocolError):
        observe(conn, e, [record("c")], 3, scope={"a"})


def test_restart_and_idle_make_no_adapter_reads(setup):
    conn, e, runner, calls = setup
    runner.tick(100)
    assert calls[-1]["items"] == ["a", "b"]
    calls.clear()
    for now in range(105, 900, 5):
        runner.tick(now)
    assert calls == []
    restarted = inc.ChangeRunner(e, conn, Sink())
    calls.clear()  # --describe is local metadata, not a graph read
    restarted.tick(1000)
    assert calls == []


def test_deadlines_survive_restart(setup):
    conn, e, runner, calls = setup
    runner.tick(100)
    conn.execute("INSERT INTO adapter_deadlines (emitter,item,due) VALUES ('e','a',500)")
    restarted = inc.ChangeRunner(e, conn, Sink())
    calls.clear()
    restarted.tick(499)
    assert not calls
    restarted.tick(500)
    assert calls[-1]["items"] == ["a"]
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines").fetchone()[0] == 0


def test_failure_keeps_pending_then_atomic_retry(setup, monkeypatch):
    conn, e, runner, calls = setup
    runner.tick(100)
    conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change()),))
    original = inc.observe
    def failure(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("crash before inbox acknowledgement")
    monkeypatch.setattr(inc, "observe", failure)
    runner.tick(110)
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines WHERE item='a'").fetchone()[0] == 1
    assert conn.execute("SELECT updated FROM verdicts WHERE item='a'").fetchone()[0] == 100
    monkeypatch.setattr(inc, "observe", original)
    runner.tick(170)
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0
    assert conn.execute("SELECT updated FROM verdicts WHERE item='a'").fetchone()[0] == 170


class Feed:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.since = []
    def _request(self, method, path):
        self.since.append(int(parse_qs(urlparse(path).query)["since"][0]))
        return next(self.pages)


def test_tail_before_discovery_and_commit_inbox_with_cursor(setup):
    conn, _, runner, _ = setup
    q = Feed([{"records": [], "next_tx": 999, "watermark_tx": 10},
              {"records": [change(11), change(12, graph="inferred")], "next_tx": 12, "watermark_tx": 12}])
    feed = inc.ChangeFeed(q, [runner])
    feed.poll()
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    feed.poll()
    assert q.since == [9223372036854775807, 10]
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 12
    assert conn.execute("SELECT tx FROM change_inbox").fetchall() == [(11,)]


def test_bad_page_cannot_advance_cursor(setup):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    feed = inc.ChangeFeed(Feed([{"records": [change(12)], "next_tx": 11, "watermark_tx": 12}]), [runner])
    feed.poll()
    assert feed.errors == 1
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0


def test_change_queue_preempts_daily_catalogue(setup):
    conn, _, runner, calls = setup
    runner.reconcile(100)
    conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('e',?,100,2)",
                     [(f"old-{i}",) for i in range(20)])
    conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change(entity="urgent")),))
    runner.tick(110)
    assert "urgent" in calls[-1]["items"]
    assert len(calls[-1]["items"]) == inc.BATCH
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines").fetchone()[0] > 0


def test_routing_failure_keeps_fact_changes(setup, monkeypatch):
    conn, _, runner, _ = setup
    runner.tick(100)
    conn.execute("INSERT INTO change_inbox VALUES ('e',2,0,?)", (json.dumps(change()),))
    def failure(*args, **kwargs):
        raise RuntimeError("reader unavailable")
    monkeypatch.setattr(inc, "call", failure)
    runner.tick(110)
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 1


def test_database_refusal_rolls_back_cursor_and_inbox(setup):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    conn.executescript("CREATE TRIGGER refuse_cursor BEFORE INSERT ON change_cursor "
                       "BEGIN SELECT RAISE(ABORT,'disk failure'); END;")
    feed = inc.ChangeFeed(Feed([{"records": [change(11)], "next_tx": 11, "watermark_tx": 11}]), [runner])
    feed.poll()
    assert feed.errors == 1
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 10
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 0


def test_factless_transactions_do_not_create_permanent_lag(setup):
    conn, _, runner, _ = setup
    conn.execute("INSERT INTO change_cursor VALUES (1,10)")
    feed = inc.ChangeFeed(Feed([{"records": [], "next_tx": 10, "watermark_tx": 12}]), [runner])
    feed.poll()
    assert feed.lag == 0
    assert conn.execute("SELECT tx FROM change_cursor").fetchone()[0] == 12
