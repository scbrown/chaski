"""Command sink — deliver an emitter event to an external command.

The domain work some events need (classify, propose, write) belongs in the
project that owns that domain, not in chaski. A verdict adapter is already an
external command; this sink is its delivery-side twin. It runs `argv` with the
frozen event as JSON on stdin, and exit 0 is the receipt.

The receiver decides exactly-once, as for every sink: the command must be
idempotent on the event id, because a crash between its success and the
emitter's checkpoint re-sends the same event. A non-zero exit, a timeout or a
failure to start raises, and the emitter retries with backoff.
"""
from __future__ import annotations

import json
import subprocess


class CommandSink:
    def __init__(self, argv: list[str], timeout_s: float = 120.0):
        if not argv:
            raise ValueError("command sink needs argv")
        self.argv, self.timeout_s = list(argv), timeout_s

    def deliver(self, event: dict) -> None:
        try:
            done = subprocess.run(self.argv, input=json.dumps(event, sort_keys=True),
                                  capture_output=True, text=True, timeout=self.timeout_s,
                                  check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ConnectionError(f"{self.argv[0]}: {exc}") from exc
        if done.returncode != 0:
            raise ConnectionError(f"{self.argv[0]} exited {done.returncode}: "
                                  f"{(done.stderr or done.stdout).strip()[:300]}")
