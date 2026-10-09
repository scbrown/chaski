"""Local event delivery proof using the production emitter and JSONL receiver."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import emitter


def verify_event(root: Path, marker: str) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    work = root / "chaski-event-proof"
    work.mkdir()  # Never reuse or replace a caller's existing database.
    ledger = work / "events.jsonl"
    conn = emitter.connect(work / "emitter.db")
    sink = emitter.JsonlSink(ledger)
    try:
        if sink.ids():
            raise RuntimeError("negative receiver control failed")
        state = work / "verdict.json"
        adapter = "import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())"
        item = emitter.Emitter("package-proof", [sys.executable, "-c", adapter, str(state)], 1, "local")
        runner = emitter.Runner(item, conn, sink, emitter.WriteBudget(1))
        state.write_text(json.dumps([{"item": marker, "verdict": "BLOCKED"}]))
        runner.tick(1000)
        if sink.ids() or conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]:
            raise RuntimeError("baseline unexpectedly emitted")
        state.write_text(json.dumps([{"item": marker, "verdict": "UNBLOCKED", "event_id": marker}]))
        runner.tick(1002)
        row = conn.execute("SELECT payload, delivered_at FROM outbox WHERE event_id=?", (marker,)).fetchone()
        if not row or row[1] is None or sink.ids() != {marker}:
            raise RuntimeError("event did not reach the receiver")
        event = json.loads(row[0])
        sink.deliver(event)  # Receiver retry must preserve exactly one event.
        lines = [json.loads(line) for line in ledger.read_text().splitlines()]
        if len(lines) != 1 or lines[0]["event_id"] != marker or runner.runs != {"ok": 2, "unknown": 0}:
            raise RuntimeError("event round trip or replay control failed")
        return {"verified": True, "receiver": "jsonl", "events": 1, "database": str(work / "emitter.db")}
    finally:
        conn.close()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    proof = sub.add_parser("verify-event", help="emit and receive an isolated local event")
    proof.add_argument("--directory", type=Path, required=True)
    proof.add_argument("--marker", required=True)
    read = sub.add_parser("events", help="read event IDs from the local proof receiver")
    read.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "events":
        print(json.dumps(sorted(emitter.JsonlSink(args.directory / "chaski-event-proof" / "events.jsonl").ids())))
        return 0
    try:
        result = verify_event(args.directory, args.marker)
    except (OSError, ValueError, RuntimeError) as error:
        print(f"verification failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0
