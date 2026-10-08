"""Durable jobs for explicit events, independent of verdict baselines.

An authenticated source calls enqueue() before acknowledging its event. A trusted
configured receiver handles the frozen envelope and returns a receipt for this
exact event/job identity. Receivers MUST be idempotent under that identity: a
crash after their side effect but before our commit replays the same envelope.
No shell command or executable is selected by event contents.
"""
from __future__ import annotations

import fcntl
import json
import sqlite3
import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

MAX_EVENT_BYTES = 64 * 1024


class EventJobs:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.db = sqlite3.connect(self.path, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS job_events (
                event_id TEXT PRIMARY KEY, envelope TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS event_jobs (
                event_id TEXT NOT NULL, job TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt REAL NOT NULL DEFAULT 0, completed_at REAL,
                PRIMARY KEY(event_id, job)
            );
        """)

    def enqueue(self, envelope: dict, jobs: list[str]) -> dict:
        if not isinstance(envelope, dict) or set(envelope) != {"event_id", "type", "payload"}:
            raise ValueError("event envelope needs exactly event_id, type and payload")
        event_id = envelope["event_id"]
        if not isinstance(event_id, str) or not event_id or len(event_id) > 512:
            raise ValueError("invalid event identity")
        if not isinstance(envelope["type"], str) or not envelope["type"]:
            raise ValueError("invalid event type")
        if not isinstance(envelope["payload"], dict):
            raise ValueError("event payload must be an object")
        if not jobs or any(not isinstance(j, str) or not j or len(j) > 128 for j in jobs):
            raise ValueError("at least one configured job is required")
        encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode()) > MAX_EVENT_BYTES:
            raise ValueError("event exceeds queue limit")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute("SELECT envelope FROM job_events WHERE event_id=?", (event_id,)).fetchone()
            if row and row[0] != encoded:
                raise ValueError("event identity cannot change its payload")
            self.db.execute("INSERT OR IGNORE INTO job_events VALUES (?,?)", (event_id, encoded))
            for job in jobs:
                self.db.execute("INSERT OR IGNORE INTO event_jobs(event_id,job) VALUES (?,?)", (event_id, job))
        return {"event_id": event_id, "received": True}

    def run_one(self, receivers: dict, now: float, *, enabled: bool = False,
                hold: Path | None = None) -> str:
        """One bounded delivery to a configured receiver; default is unarmed.

        The receiver owns its execution timeout and checks installation holds
        again immediately before changing a host. Chaski's hold prevents new
        delivery, but cannot retract an already-running external action.
        """
        if not enabled or (hold is not None and hold.exists()):
            return "held"
        with self.path.with_suffix(self.path.suffix + ".worker-lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return "busy"
            row = self.db.execute("""
                SELECT j.event_id,j.job,e.envelope,j.attempts
                FROM event_jobs j JOIN job_events e USING(event_id)
                WHERE j.state != 'complete' AND j.next_attempt <= ?
                ORDER BY j.next_attempt,j.event_id,j.job LIMIT 1
            """, (now,)).fetchone()
            if row is None:
                return "idle"
            event_id, job, encoded, attempts = row
            if job not in receivers:
                # A removed/unknown job is not an acknowledged delivery.
                return "unknown"
            with self.db:
                self.db.execute("UPDATE event_jobs SET state='running',attempts=attempts+1 "
                                "WHERE event_id=? AND job=?", (event_id, job))
            try:
                receipt = receivers[job](json.loads(encoded))
                if not isinstance(receipt, dict) or receipt.get("event_id") != event_id or receipt.get("job") != job:
                    raise ValueError("receiver did not prove the exact event and job")
                outcome = receipt.get("outcome")
                if outcome not in {"complete", "held", "unknown", "failed"}:
                    raise ValueError("receiver omitted a known outcome")
            except Exception:  # receiver failure is retryable, never completion
                outcome = "unknown"
            with self.db:
                if outcome == "complete":
                    self.db.execute("UPDATE event_jobs SET state='complete',completed_at=? "
                                    "WHERE event_id=? AND job=?", (now, event_id, job))
                else:
                    delay = min(3600, 5 * 2 ** min(attempts, 10))
                    self.db.execute("UPDATE event_jobs SET state=?,next_attempt=? WHERE event_id=? AND job=?",
                                    (outcome, now + delay, event_id, job))
            return outcome

    def pending(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM event_jobs WHERE state != 'complete'").fetchone()[0]

    def close(self):
        self.db.close()


def configuration(path: Path) -> dict:
    config = json.loads(path.read_text())
    if not isinstance(config, dict) or set(config) - {"enabled", "hold_file", "jobs"}:
        raise ValueError("unknown event-job configuration fields")
    if type(config.get("enabled", False)) is not bool:
        raise ValueError("enabled must be a boolean")
    hold = config.get("hold_file")
    if not isinstance(hold, str) or not Path(hold).is_absolute():
        raise ValueError("an explicit absolute host hold path is required")
    jobs = config.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        raise ValueError("configured jobs are required")
    for job, spec in jobs.items():
        if not isinstance(job, str) or not job or not isinstance(spec, dict):
            raise ValueError("invalid configured job")
        if set(spec) != {"types", "command", "timeout"}:
            raise ValueError("each job needs exactly types, command and timeout")
        if not isinstance(spec["types"], list) or not spec["types"] or any(
                not isinstance(t, str) or not t for t in spec["types"]):
            raise ValueError("job needs explicit event types")
        command = spec["command"]
        if not isinstance(command, list) or not command or any(not isinstance(a, str) for a in command):
            raise ValueError("job command must be an argument array")
        if not Path(command[0]).is_absolute():
            raise ValueError("job executable must use an absolute path")
        if type(spec["timeout"]) is not int or not 1 <= spec["timeout"] <= 600:
            raise ValueError("receiver timeout must be between 1 and 600 seconds")
    return config


def execute(spec: dict, envelope: dict) -> dict:
    """Fixed operator-reviewed argv; event contents go only to stdin.

    Private output files avoid retaining arbitrary subprocess output in RAM or
    echoing credentials. Killing the whole process group bounds a timed-out
    receiver and its children before another delivery can begin.
    """
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        proc = subprocess.Popen(spec["command"], stdin=subprocess.PIPE, stdout=output,
                                stderr=errors, start_new_session=True)
        try:
            proc.communicate(json.dumps(envelope).encode(), timeout=spec["timeout"])
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            raise RuntimeError("event receiver timed out") from None
        if proc.returncode != 0:
            raise RuntimeError("event receiver failed")
        output.seek(0)
        raw = output.read(MAX_EVENT_BYTES + 1)
        if len(raw) > MAX_EVENT_BYTES:
            raise ValueError("receiver receipt exceeds limit")
        return json.loads(raw)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["enqueue", "run-once"])
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)
    config = configuration(args.config)
    args.state.parent.mkdir(parents=True, exist_ok=True)
    queue = EventJobs(args.state)
    try:
        if args.operation == "enqueue":
            raw = sys.stdin.buffer.read(MAX_EVENT_BYTES + 1)
            if len(raw) > MAX_EVENT_BYTES:
                raise ValueError("event exceeds queue limit")
            event = json.loads(raw)
            if not isinstance(event, dict):
                raise ValueError("event envelope must be an object")
            jobs = [name for name, spec in config["jobs"].items() if event.get("type") in spec["types"]]
            # Queue reception is permitted while execution is held. The event
            # is retained for when the operator releases timing, not dropped.
            result = queue.enqueue(event, jobs)
        else:
            receivers = {name: (lambda event, spec=spec: execute(spec, event))
                         for name, spec in config["jobs"].items()}
            result = {"outcome": queue.run_one(receivers, time.time(), enabled=config.get("enabled", False),
                                              hold=Path(config["hold_file"]))}
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        queue.close()


if __name__ == "__main__":
    raise SystemExit(main())
