"""chaski emitter — turn a verdict adapter's output into transition events,
delivered at least once to an idempotent receiver.

Stage 2. A verdict adapter is an external command that prints a JSON list of
records, one per tracked item:

    {"item": "...", "verdict": "BLOCKED" | "UNBLOCKED" | "UNKNOWN",
     "event_id": "...",          # required on UNBLOCKED
     "evidence": "...", "assignee": "...", ...}

The adapter is stateless. Everything that makes a transition an EVENT lives
here, in one SQLite file:

- A first observation of an item is a BASELINE, never an event: we did not see
  it become unblocked, we only met it that way.
- UNKNOWN keeps the last known verdict and never emits. "Could not tell" must
  not round to "unblocked", and BLOCKED -> UNKNOWN -> UNBLOCKED emits exactly
  once, when the positive evidence returns.
- Only a known BLOCKED -> UNBLOCKED emits. Every verdict change advances the
  item's generation, so a later genuine re-block and unblock is a new event.
- An item missing from a complete run is UNTRACKED (closed, or no blockers
  left), never unblocked. If it comes back, that is a new baseline.
- A failed run (non-zero exit, timeout, unparseable output) is UNKNOWN for
  every item: nothing changes.

The verdict update and the outbox row commit in ONE transaction, so a crash
cannot lose an event or record one twice. The outbox is keyed on the
adapter's event id, which must be DETERMINISTIC from the item and the
transition that caused it, never minted at send time. A re-detected
transition after a crash then produces the same id and INSERT OR IGNORE keeps
one row.

Delivery is at least once. A row is marked delivered only after the sink
returned, which it does only on an observed receipt; a failure is retried
with backoff. A crash between a successful send and the checkpoint re-sends
the same id, and the receiver deduplicates on it. That is what makes the
receiver, not this sender, the place exactly-once is decided.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

LOG = logging.getLogger("chaski.emitter")

BLOCKED, UNBLOCKED, UNKNOWN = "BLOCKED", "UNBLOCKED", "UNKNOWN"
MAX_BACKOFF_S = 3600
BASE_BACKOFF_S = 30
# Deliveries per tick. The reactor ticks every 5 s, so this bounds a backlog
# drain at 1 write/s: quipu applies writes one at a time, and a new writer
# arriving in a burst has wedged it before.
DELIVER_PER_TICK = 5

SCHEMA = """
CREATE TABLE IF NOT EXISTS verdicts (
    emitter    TEXT NOT NULL,
    item       TEXT NOT NULL,
    verdict    TEXT NOT NULL,
    generation INTEGER NOT NULL,
    evidence   TEXT,
    tracked    INTEGER NOT NULL DEFAULT 1,
    last_event TEXT,
    updated    REAL NOT NULL,
    PRIMARY KEY (emitter, item)
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id     TEXT PRIMARY KEY,
    emitter      TEXT NOT NULL,
    item         TEXT NOT NULL,
    generation   INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    created      REAL NOT NULL,
    delivered_at REAL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL NOT NULL DEFAULT 0,
    last_error   TEXT
);
"""


class ProtocolError(ValueError):
    """The adapter's output breaks the record contract. The whole run is refused."""


@dataclass
class Emitter:
    """One adapter, run on a schedule.

    `key` names the record field that identifies an item ("item" for the
    blocked-by adapter, "entity" for the review-age one). A transition from
    `from_verdict` to `to_verdict` is the event, named `event` for consumers.
    `baseline_emits` makes a first sighting already in `to_verdict` an event
    too: right for "this is overdue" (the owner must hear it however late we
    met it), wrong for "this became unblocked" (we did not see it happen).
    """

    label: str
    command: list[str]
    interval_s: int
    owner: str
    event: str = "unblocked"
    timeout_s: int = 300
    key: str = "item"
    from_verdict: str = BLOCKED
    to_verdict: str = UNBLOCKED
    baseline_emits: bool = False

    @property
    def verdicts(self) -> set[str]:
        return {self.from_verdict, self.to_verdict, UNKNOWN}


def connect(path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), isolation_level=None)  # explicit transactions below
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(verdicts)")}
    if "last_event" not in cols:  # a state file from before last_event existed
        conn.execute("ALTER TABLE verdicts ADD COLUMN last_event TEXT")
    return conn


def run_adapter(emitter: Emitter) -> list[dict]:
    """The adapter's records. Raises on ANY failure: the caller treats it as UNKNOWN."""
    proc = subprocess.run(emitter.command, capture_output=True, text=True,
                          timeout=emitter.timeout_s, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"adapter exited {proc.returncode}: {proc.stderr.strip()[:300]}")
    records = json.loads(proc.stdout)
    if not isinstance(records, list):
        raise ProtocolError("adapter output is not a JSON list")
    return records


def _validate(emitter: Emitter, records: list[dict]) -> None:
    seen = set()
    for r in records:
        if not isinstance(r, dict) or not isinstance(r.get(emitter.key), str) or not r[emitter.key]:
            raise ProtocolError(f"record without {emitter.key!r}: {str(r)[:120]}")
        item = r[emitter.key]
        if r.get("verdict") not in emitter.verdicts:
            raise ProtocolError(f"{item}: verdict {r.get('verdict')!r} not in {sorted(emitter.verdicts)}")
        if r["verdict"] == emitter.to_verdict and not r.get("event_id"):
            # Without a deterministic id the receiver cannot deduplicate, and a
            # send-time id would make at-least-once delivery at-least-twice.
            raise ProtocolError(f"{item}: {emitter.to_verdict} without event_id")
        if item in seen:
            raise ProtocolError(f"{item}: listed twice in one run")
        seen.add(item)


def observe(conn: sqlite3.Connection, emitter: Emitter, records: list[dict], now: float) -> list[str]:
    """Apply one COMPLETE adapter run. Returns the event ids newly queued.

    All-or-nothing: a protocol error leaves every verdict and the outbox as
    they were.
    """
    _validate(emitter, records)
    queued: list[str] = []

    def emit(r: dict, item: str, generation: int) -> None:
        payload = {
            "event_id": r["event_id"], "event": emitter.event, "emitter": emitter.label,
            "item": item, "generation": generation,
            # whoever the adapter says should act: the assignee of a work item,
            # the owner of an aged entity
            "recipient": r.get("assignee") or r.get("owner"),
            "evidence": r.get("evidence"), "observed_at": now,
        }
        cur = conn.execute(
            "INSERT OR IGNORE INTO outbox (event_id, emitter, item, generation, payload, created)"
            " VALUES (?,?,?,?,?,?)",
            (r["event_id"], emitter.label, item, generation, json.dumps(payload, sort_keys=True), now))
        conn.execute("UPDATE verdicts SET last_event=? WHERE emitter=? AND item=?",
                     (r["event_id"], emitter.label, item))
        if cur.rowcount:
            queued.append(r["event_id"])

    conn.execute("BEGIN IMMEDIATE")
    try:
        present = set()
        for r in records:
            item, verdict = r[emitter.key], r["verdict"]
            present.add(item)
            row = conn.execute(
                "SELECT verdict, generation, tracked, last_event FROM verdicts WHERE emitter=? AND item=?",
                (emitter.label, item)).fetchone()
            if row is None or not row[2]:
                if verdict == UNKNOWN:
                    continue  # nothing known to baseline from
                conn.execute(
                    "INSERT INTO verdicts (emitter, item, verdict, generation, evidence, tracked, updated)"
                    " VALUES (?,?,?,?,?,1,?) ON CONFLICT(emitter, item) DO UPDATE SET"
                    " verdict=excluded.verdict, evidence=excluded.evidence, tracked=1,"
                    " generation=generation+1, updated=excluded.updated",
                    (emitter.label, item, verdict, 0, r.get("evidence"), now))
                if emitter.baseline_emits and verdict == emitter.to_verdict:
                    gen = conn.execute("SELECT generation FROM verdicts WHERE emitter=? AND item=?",
                                       (emitter.label, item)).fetchone()[0]
                    emit(r, item, gen)
                continue
            last, generation, last_event = row[0], row[1], row[3]
            if verdict == UNKNOWN:
                continue
            if verdict == last:
                conn.execute("UPDATE verdicts SET evidence=?, updated=? WHERE emitter=? AND item=?",
                             (r.get("evidence"), now, emitter.label, item))
                # Same verdict, NEW transition: it went away and came back
                # between two runs (re-blocked and unblocked again; re-verified
                # and lapsed again). The adapter's id says so; the verdict cannot.
                if (verdict == emitter.to_verdict and last_event is not None
                        and r["event_id"] != last_event):
                    generation += 2
                    conn.execute("UPDATE verdicts SET generation=? WHERE emitter=? AND item=?",
                                 (generation, emitter.label, item))
                    emit(r, item, generation)
                continue
            generation += 1
            conn.execute(
                "UPDATE verdicts SET verdict=?, generation=?, evidence=?, updated=? WHERE emitter=? AND item=?",
                (verdict, generation, r.get("evidence"), now, emitter.label, item))
            if last == emitter.from_verdict and verdict == emitter.to_verdict:
                emit(r, item, generation)
        for (item,) in conn.execute(
                "SELECT item FROM verdicts WHERE emitter=? AND tracked=1", (emitter.label,)).fetchall():
            if item not in present:
                conn.execute("UPDATE verdicts SET tracked=0, updated=? WHERE emitter=? AND item=?",
                             (now, emitter.label, item))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return queued


class Sink(Protocol):
    """Deliver one event. Return ONLY after an observed receipt; raise otherwise."""

    def deliver(self, event: dict) -> None: ...


class JsonlSink:
    """An idempotent receiver: a JSONL file that keeps one line per event id.

    The reference receiver, and the form consumers can read before a graph sink
    exists. Deduplication is the RECEIVER's job, so it is here.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)

    def ids(self) -> set[str]:
        if not self.path.exists():
            return set()
        return {json.loads(line)["event_id"] for line in self.path.read_text().splitlines() if line}

    def deliver(self, event: dict) -> None:
        if event["event_id"] in self.ids():
            return  # already received: the re-send after a lost checkpoint
        with self.path.open("a") as f:
            f.write(json.dumps(event, sort_keys=True) + "\n")
            f.flush()


class WriteBudget:
    """ONE budget shared by every emitter in the process: at most one sink
    write per `interval_s`, globally. quipu applies writes one at a time, so the
    bound that matters is the reactor's, not each emitter's. A retry spends
    budget like a first attempt, and so does a sink's one-time setup write."""

    def __init__(self, interval_s: float):
        self.interval_s = interval_s
        self._last = float("-inf")
        self._lock = threading.Lock()

    def try_take(self, now: float) -> bool:
        with self._lock:
            if now - self._last < self.interval_s:
                return False
            self._last = now
            return True


def deliver_pending(conn: sqlite3.Connection, sink: Sink, now: float, emitter: str,
                    budget: WriteBudget | None = None, limit: int = 50) -> tuple[int, int]:
    """Send THIS emitter's due outbox rows to THIS emitter's sink, spending one
    budget slot per write. Returns (delivered, failed).

    Scoped by emitter because runners share one state database: an unscoped
    pass would hand another emitter's events to this sink and checkpoint them
    there, so their own sink would never see them."""
    # A sink may owe a one-time setup write (the graph sink's Reaction). It
    # takes its own budget slot, never piggy-backing on an event's.
    setup = getattr(sink, "setup_pending", None)
    if setup is not None and setup():
        if budget is not None and not budget.try_take(now):
            return 0, 0
        try:
            sink.setup()
        except Exception as exc:  # noqa: BLE001 — retried next pass, no row is charged
            LOG.warning("sink setup for %s failed: %s", emitter, exc)
            return 0, 1
    rows = conn.execute(
        "SELECT event_id, payload, attempts FROM outbox WHERE emitter = ? AND delivered_at IS NULL"
        " AND next_attempt <= ? ORDER BY created LIMIT ?", (emitter, now, limit)).fetchall()
    ok = failed = 0
    for event_id, payload, attempts in rows:
        if budget is not None and not budget.try_take(now):
            break  # the rest wait for a later slot; nothing is charged to them
        try:
            sink.deliver(json.loads(payload))
        except Exception as exc:  # noqa: BLE001 — any failure is a retry
            failed += 1
            backoff = min(MAX_BACKOFF_S, BASE_BACKOFF_S * 2 ** attempts)
            conn.execute("UPDATE outbox SET attempts=attempts+1, next_attempt=?, last_error=?"
                         " WHERE event_id=? AND emitter=?",
                         (now + backoff, str(exc)[:300], event_id, emitter))
            LOG.warning("delivery of %s failed (attempt %d): %s", event_id, attempts + 1, exc)
            continue
        # Only now, after the sink returned on a receipt. A crash before this
        # line re-sends the same id next time; the receiver deduplicates.
        conn.execute("UPDATE outbox SET delivered_at=?, attempts=attempts+1 WHERE event_id=? AND emitter=?",
                     (now, event_id, emitter))
        ok += 1
    return ok, failed


class Runner:
    """Schedules one emitter's adapter runs and deliveries, and counts them.

    tick() runs on the reactor's thread and may take minutes (the adapter).
    metrics() runs on the HTTP thread and must never block on it or touch the
    SQLite connection (which belongs to the ticking thread), so tick()
    publishes a snapshot under a small lock and metrics() only reads that."""

    def __init__(self, emitter: Emitter, conn: sqlite3.Connection, sink: Sink,
                 budget: WriteBudget | None = None):
        self.emitter, self.conn, self.sink, self.budget = emitter, conn, sink, budget
        self.last_attempt = 0.0
        self.last_success = 0.0
        self.runs = {"ok": 0, "unknown": 0}
        self.queued_total = 0
        self._snap_lock = threading.Lock()
        self._snapshot: dict = {}
        self._publish()

    def tick(self, now: float) -> None:
        if now - self.last_attempt >= self.emitter.interval_s:
            self.last_attempt = now
            try:
                records = run_adapter(self.emitter)
                self.queued_total += len(observe(self.conn, self.emitter, records, now))
                self.runs["ok"] += 1
                self.last_success = now
            except Exception as exc:  # noqa: BLE001 — a failed run is UNKNOWN for every item
                self.runs["unknown"] += 1
                LOG.warning("emitter %s UNKNOWN (no verdict changed): %s", self.emitter.label, exc)
        deliver_pending(self.conn, self.sink, now, self.emitter.label, self.budget, limit=DELIVER_PER_TICK)
        self._publish()

    def _publish(self) -> None:
        pending, oldest = self.conn.execute(
            "SELECT COUNT(*), MIN(created) FROM outbox WHERE emitter=? AND delivered_at IS NULL",
            (self.emitter.label,)).fetchone()
        delivered = self.conn.execute(
            "SELECT COUNT(*) FROM outbox WHERE emitter=? AND delivered_at IS NOT NULL",
            (self.emitter.label,)).fetchone()[0]
        snap = {"runs": dict(self.runs), "last_success": self.last_success,
                "queued_total": self.queued_total, "pending": pending,
                "oldest_created": oldest, "delivered": delivered}
        with self._snap_lock:
            self._snapshot = snap

    def metrics(self, esc) -> list[str]:
        with self._snap_lock:
            s = dict(self._snapshot)
        label = esc(self.emitter.label)
        lines = [f'chaski_emitter_runs_total{{emitter="{label}",result="{r}"}} {n}' for r, n in s["runs"].items()]
        oldest = s["oldest_created"]
        lines += [
            f'chaski_emitter_last_success_timestamp_seconds{{emitter="{label}"}} {s["last_success"]:.0f}',
            f'chaski_emitter_events_queued_total{{emitter="{label}"}} {s["queued_total"]}',
            f'chaski_emitter_outbox_pending{{emitter="{label}"}} {s["pending"]}',
            f'chaski_emitter_outbox_delivered_total{{emitter="{label}"}} {s["delivered"]}',
            f'chaski_emitter_outbox_oldest_pending_age_seconds{{emitter="{label}"}} '
            f'{(time.time() - oldest) if oldest else 0:.0f}',
        ]
        return lines
