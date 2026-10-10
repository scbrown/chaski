"""Durable fact-change inbox and bounded, deadline-driven verdict evaluation.

Cursor advancement and inbox insertion are one transaction. Inbox acknowledgement,
deadlines, verdicts and outbox insertion are another. A crash can replay work but
cannot skip it. Reconciliation only discovers keys; evaluation drains in batches
so a daily reconciliation never monopolizes the reactor with a population scan.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import subprocess
import urllib.parse

from emitter import DELIVER_PER_TICK, ProtocolError, Runner, deliver_pending, observe

LOG = logging.getLogger("chaski.changes")
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
BATCH = 2
SCHEMA = """
CREATE TABLE IF NOT EXISTS change_cursor (id INTEGER PRIMARY KEY CHECK(id=1), tx INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS change_inbox (
 emitter TEXT NOT NULL, tx INTEGER NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(emitter,tx,seq));
CREATE TABLE IF NOT EXISTS adapter_deadlines (
 emitter TEXT NOT NULL, item TEXT NOT NULL, due REAL NOT NULL, priority INTEGER NOT NULL DEFAULT 1,
 PRIMARY KEY(emitter,item));
CREATE TABLE IF NOT EXISTS adapter_reconcile (
 emitter TEXT PRIMARY KEY, last_success REAL NOT NULL);
CREATE TABLE IF NOT EXISTS adapter_subscription (
 emitter TEXT PRIMARY KEY, digest TEXT NOT NULL);
"""


def call(emitter, request=None):
    flag = "--describe" if request is None else "--changes"
    proc = subprocess.run([*emitter.command, flag], input=json.dumps(request) if request is not None else None,
                          capture_output=True, text=True, timeout=emitter.timeout_s, check=False)
    if proc.returncode:
        raise RuntimeError(f"adapter exited {proc.returncode}: {proc.stderr[:300]}")
    out = json.loads(proc.stdout)
    if not isinstance(out, dict) or out.get("version") != 1:
        raise ProtocolError("expected incremental adapter protocol version 1")
    return out


def strings(value):
    if not isinstance(value, list) or any(not isinstance(v, str) or not v for v in value):
        raise ProtocolError("expected a list of nonempty strings")
    return set(value)


class ChangeRunner(Runner):
    def __init__(self, emitter, conn, sink, budget=None):
        conn.executescript(SCHEMA)
        self.subscription = call(emitter)
        self.attributes = strings(self.subscription.get("attributes"))
        self.graphs = strings(self.subscription.get("graphs"))
        self.types = strings(self.subscription.get("types", []))
        self.retry_at = 0.0
        self._reconcile_on_new_subscription(conn, emitter.label)
        super().__init__(emitter, conn, sink, budget)

    def _reconcile_on_new_subscription(self, conn, label):
        """A changed subscription reconciles at once, not on the daily pass.

        Changes to an attribute the OLD subscription did not include were
        consumed while the old adapter ran and the cursor moved past them, so
        nothing routes them again. Measured 2026-10-08 (aegis-z1epad): 68
        WorkItems gained a newly subscribed attribute, 20 of them due, and
        chaski saw none of them for most of a day. Forgetting the last
        reconcile makes the next tick rediscover every candidate; the
        verdict/outbox state keeps that idempotent. Also true on the first start
        after this change, where no digest has been stored yet."""
        digest = hashlib.sha256(json.dumps(self.subscription, sort_keys=True).encode()).hexdigest()
        row = conn.execute("SELECT digest FROM adapter_subscription WHERE emitter=?", (label,)).fetchone()
        if row and row[0] == digest:
            return
        conn.execute("DELETE FROM adapter_reconcile WHERE emitter=?", (label,))
        conn.execute("INSERT OR REPLACE INTO adapter_subscription VALUES (?,?)", (label, digest))
        conn.commit()

    def accepts(self, record):
        if record["graph"] not in self.graphs:
            return False
        if record["attribute"] in self.attributes:
            return True
        return record["attribute"] == TYPE and any(
            isinstance(record.get(k), dict) and record[k].get("ref") in self.types
            for k in ("value", "old_value"))

    def reconcile(self, now):
        row = self.conn.execute("SELECT last_success FROM adapter_reconcile WHERE emitter=?",
                                (self.emitter.label,)).fetchone()
        if row and now - row[0] < self.emitter.interval_s:
            return
        items = strings(call(self.emitter, {"discover": True, "now": now}).get("items"))
        label = self.emitter.label
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            tracked = {r[0] for r in self.conn.execute("SELECT item FROM verdicts WHERE emitter=? AND tracked=1",
                                                       (label,))}
            observe(self.conn, self.emitter, [], now, scope=tracked - items)
            self.conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES (?,?,?,2)"
                                  " ON CONFLICT(emitter,item)"
                                  " DO UPDATE SET due=MIN(due,excluded.due)",
                                  [(label, item, now) for item in items])
            self.conn.execute("INSERT OR REPLACE INTO adapter_reconcile VALUES (?,?)", (label, now))
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def route_pending(self, now):
        label = self.emitter.label
        pending = self.conn.execute(
            "SELECT tx,seq,payload FROM change_inbox WHERE emitter=? AND json_extract(payload,'$.entity') IN "
            "(SELECT json_extract(payload,'$.entity') FROM change_inbox WHERE emitter=? "
            " GROUP BY json_extract(payload,'$.entity') ORDER BY MIN(tx),MIN(seq) LIMIT ?) ORDER BY tx,seq",
            (label, label, BATCH)).fetchall()
        if not pending:
            return
        items = strings(call(self.emitter, {"now": now, "route": [json.loads(r[2]) for r in pending]}).get("items"))
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES (?,?,?,0)"
                                  " ON CONFLICT(emitter,item) DO UPDATE SET due=MIN(due,excluded.due), priority=0",
                                  [(label, item, now) for item in items])
            self.conn.executemany("DELETE FROM change_inbox WHERE emitter=? AND tx=? AND seq=?",
                                  [(label, row[0], row[1]) for row in pending])
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def evaluate_pending(self, now):
        label = self.emitter.label
        items = [r[0] for r in self.conn.execute(
            "SELECT item FROM adapter_deadlines WHERE emitter=? AND due<=? ORDER BY priority,due,item LIMIT ?",
            (label, now, BATCH))]
        if not items:
            return
        result = call(self.emitter, {"now": now, "items": items,
                                    "changes": []})
        scope = strings(result.get("scope"))
        records, deadlines = result.get("records"), result.get("next_checks")
        if not isinstance(records, list) or not isinstance(deadlines, dict) or not set(items) <= scope:
            raise ProtocolError("partial result must cover the requested keys")
        if set(deadlines) != {r.get(self.emitter.key) for r in records if isinstance(r, dict)} or any(t is not None and
                (isinstance(t, bool) or not isinstance(t, (float, int)) or not math.isfinite(t) or t <= now)
                for t in deadlines.values()):
            raise ProtocolError("invalid future deadline")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            queued = observe(self.conn, self.emitter, records, now, scope=scope)
            self.conn.executemany("DELETE FROM adapter_deadlines WHERE emitter=? AND item=?",
                                  [(label, item) for item in scope])
            self.conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due) VALUES (?,?,?)",
                                  [(label, item, due) for item, due in deadlines.items() if due is not None])
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.queued_total += len(queued)
        self.runs["ok"] += 1
        self.last_success = now

    def tick(self, now):
        if now >= self.retry_at:
            try:
                self.reconcile(now)
                self.route_pending(now)
                self.evaluate_pending(now)
            except Exception as exc:  # noqa: BLE001
                self.runs["unknown"] += 1
                self.retry_at = now + 60
                LOG.warning("incremental emitter %s UNKNOWN (work retained): %s", self.emitter.label, exc)
        deliver_pending(self.conn, self.sink, now, self.emitter.label, self.budget, limit=DELIVER_PER_TICK)
        self._publish()

    def _publish(self):
        super()._publish()
        pending = self.conn.execute("SELECT COUNT(*) FROM change_inbox WHERE emitter=?",
                                    (self.emitter.label,)).fetchone()[0]
        count, due = self.conn.execute("SELECT COUNT(*), MIN(due) FROM adapter_deadlines WHERE emitter=?",
                                      (self.emitter.label,)).fetchone()
        with self._snap_lock:
            self._snapshot.update(change_pending=pending, deadlines=count, next_due=due or 0)

    def metrics(self, esc):
        lines = super().metrics(esc)
        with self._snap_lock:
            s = dict(self._snapshot)
        for name, value in (("changes_pending", s["change_pending"]), ("deadlines", s["deadlines"]),
                            ("next_due_timestamp_seconds", s["next_due"])):
            lines.append(f'chaski_emitter_{name}{{emitter="{esc(self.emitter.label)}"}} {value}')
        return lines


class ChangeFeed:
    def __init__(self, quipu, runners):
        self.quipu, self.runners = quipu, runners
        self.conn = runners[0].conn
        self.lag = -1
        self.errors = 0
        self.ready = False

    def poll(self):
        # Bounded catch-up in one poll: a normal burst need not wait another
        # minute per page. Each page commits independently before the next read.
        for _ in range(10):
            if not self._page():
                break

    def _page(self):
        try:
            row = self.conn.execute("SELECT tx FROM change_cursor WHERE id=1").fetchone()
            # Tail before initial discovery. Subsequent writes remain after the
            # durable cursor even when the catalogue takes several ticks to drain.
            since = row[0] if row else 9223372036854775807
            query = urllib.parse.urlencode({"since": since, "capture": "old_and_new_values", "limit": 100})
            out = self.quipu._request("GET", "/changes?" + query)
            return self.accept_page(out, since)
        except Exception as exc:  # noqa: BLE001
            self.errors += 1
            LOG.warning("change feed UNKNOWN (cursor retained): %s", exc)

    def accept_page(self, out, since, stream_id=None):
        """Apply a delivered page atomically; delivery alone never advances the cursor.

        Network readers pass pages to the SQLite owner thread. Stream identifiers
        are transaction cursors, not offsets accepted by /events/commit.
        """
        row = self.conn.execute("SELECT tx FROM change_cursor WHERE id=1").fetchone()
        if row is not None and row[0] != since:
            raise ProtocolError("stale change delivery")
        records, watermark, cursor = out["records"], out["watermark_tx"], out["next_tx"]
        if not isinstance(records, list) or type(watermark) is not int or type(cursor) is not int:
            raise ProtocolError("invalid change page")
        if watermark < 0 or cursor < 0 or (row is not None and cursor > watermark):
            raise ProtocolError("change cursor outside committed prefix")
        if row and cursor < since:
            raise ProtocolError("change cursor regressed")
        if stream_id is not None and (row is None or type(stream_id) is not int
                                      or stream_id != cursor or stream_id <= since):
            raise ProtocolError("stream ID is not the durable transaction cursor")
        cursor = cursor if row and (records or stream_id is not None) else watermark
        deliveries = []
        for r in records if row else []:
            if (not isinstance(r, dict) or type(r.get("tx")) is not int
                    or type(r.get("sequence")) is not int or r["sequence"] < 0
                    or not since < r["tx"] <= cursor
                    or r.get("op") not in {"assert", "retract", "tombstone"}):
                raise ProtocolError("invalid change record")
            for runner in self.runners:
                if runner.accepts(r):
                    deliveries.append((runner.emitter.label, r["tx"], r["sequence"], json.dumps(r)))
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.executemany("INSERT OR IGNORE INTO change_inbox VALUES (?,?,?,?)", deliveries)
            self.conn.execute("INSERT OR REPLACE INTO change_cursor VALUES (1,?)", (cursor,))
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.lag = watermark - cursor
        self.ready = True
        return row is not None and cursor > since and cursor < watermark
