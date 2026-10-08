"""Commit an authenticated source's explicit event to Chaski's existing outbox.

This local CLI exposes no network listener. The source authenticates the event,
then sends its frozen envelope on stdin. A receipt is issued only after the
existing verdict/outbox transaction commits. Chaski's normal sink runner drains
the outbox, so retries, receiver deduplication and write budgets stay shared.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import emitter
from chaski import load_emitters

MAX_BYTES = 64 * 1024


def identity(label: str, source_id: str) -> str:
    key = json.dumps([label, source_id], separators=(",", ":"))
    return "chaski-external-" + hashlib.sha256(key.encode()).hexdigest()


def receive(conn, definition: emitter.Emitter, envelope: dict, now: float) -> dict:
    if not definition.externally_triggered or not definition.baseline_emits:
        raise ValueError("explicit events require an external emitter with baseline_emits")
    if not isinstance(envelope, dict) or set(envelope) != {"event_id", "type", "payload"}:
        raise ValueError("event envelope needs exactly event_id, type and payload")
    source_id = envelope["event_id"]
    if not isinstance(source_id, str) or not source_id or len(source_id) > 512:
        raise ValueError("invalid source event identity")
    if envelope["type"] != definition.event or not isinstance(envelope["payload"], dict):
        raise ValueError("event type or payload does not match the configured emitter")
    encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > MAX_BYTES:
        raise ValueError("event exceeds queue limit")
    event_id = identity(definition.label, source_id)
    # The focus can be rendered as an IRI by a graph sink: use our safe hash,
    # never an external identifier's arbitrary characters.
    record = {definition.key: event_id, "verdict": definition.to_verdict,
              "event_id": event_id, "owner": definition.owner, "evidence": encoded}
    # A source's receipt must not race another process replacing the same ID.
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("BEGIN IMMEDIATE")
    try:
        old = conn.execute("SELECT evidence FROM verdicts WHERE emitter=? AND item=?",
                           (definition.label, event_id)).fetchone()
        if old and old[0] != encoded:
            raise ValueError("source event identity cannot change its frozen payload")
        emitter.observe(conn, definition, [record], now, scope={event_id})
        row = conn.execute("SELECT payload FROM outbox WHERE emitter=? AND event_id=?",
                           (definition.label, event_id)).fetchone()
        if row is None or json.loads(row[0]).get("evidence") != encoded:
            raise ValueError("event did not reach the exact durable outbox")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return {"event_id": source_id, "received": True, "chaski_event_id": event_id}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emitters", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--state", type=Path, required=True, help="the existing emitter database")
    args = parser.parse_args(argv)
    configured = [e for e, _sink in load_emitters(args.emitters) if e.label == args.label]
    if len(configured) != 1:
        parser.error("exactly one configured emitter must match the label")
    raw = sys.stdin.buffer.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        parser.error("event exceeds queue limit")
    os.umask(0o077)
    args.state.parent.mkdir(parents=True, exist_ok=True)
    conn = emitter.connect(args.state)
    try:
        print(json.dumps(receive(conn, configured[0], json.loads(raw), time.time()), sort_keys=True))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
