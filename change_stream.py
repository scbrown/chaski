"""Bounded change-stream delivery; resume advances only after durable inbox commit."""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
import urllib.request

from emitter import ProtocolError

LOG = logging.getLogger("chaski.changes.stream")
FRAME_BYTES = 1 << 20
MAX_CURSOR = (1 << 63) - 1
BURST_PAGES = 64
BURST_SECONDS = 0.05


def frames(response):
    """Parse only complete, bounded Quipu transaction frames; heartbeats do no work."""
    fields, size = {}, 0
    while True:
        line = response.readline(FRAME_BYTES + 1)
        if not line:
            if fields:
                raise ProtocolError("truncated stream frame")
            return
        size += len(line)
        if size > FRAME_BYTES:
            raise ProtocolError("stream frame exceeds budget")
        line = line.rstrip(b'\r\n')
        if not line:
            if fields:
                if fields.get('event') != 'quipu.changes' or set(fields) != {'event', 'id', 'data'}:
                    raise ProtocolError("unexpected change stream event")
                raw = fields['id']
                if not raw.isascii() or not raw.isdecimal() or int(raw) > MAX_CURSOR:
                    raise ProtocolError("invalid transaction stream ID")
                page = json.loads(fields['data'])
                if not isinstance(page, dict):
                    raise ProtocolError("invalid transaction stream page")
                yield int(raw), page
            fields, size = {}, 0
            continue
        if line.startswith(b':'):
            continue
        key, sep, value = line.partition(b':')
        if not sep:
            raise ProtocolError("invalid stream field")
        key, value = key.decode('ascii'), value.removeprefix(b' ').decode('utf-8')
        if key not in {'event', 'id', 'data'} or key in fields:
            raise ProtocolError("duplicate or unsupported stream field")
        fields[key] = value


class ChangeStream:
    """One network reader, one queued frame; SQLite stays on the reactor thread."""
    def __init__(self, quipu, cursor):
        self.quipu, self.cursor = quipu, cursor
        self.pending = queue.Queue(maxsize=1)
        self.errors = 0
        self.stop = threading.Event()
        self.wakeup = threading.Event()
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        backoff = 1
        while not self.stop.is_set():
            try:
                req = urllib.request.Request(
                    self.quipu.base + '/changes/stream?since=' + str(self.cursor),
                    headers={'Accept': 'text/event-stream', 'X-Quipu-Client': 'chaski',
                             'Last-Event-ID': str(self.cursor)})
                # Heartbeats arrive every15s; a stalled connection reconnects boundedly.
                with urllib.request.urlopen(req, timeout=45) as response:
                    if response.headers.get_content_type() != 'text/event-stream':
                        raise ProtocolError('change stream has wrong content type')
                    for stream_id, page in frames(response):
                        done = threading.Event()
                        self.pending.put((stream_id, page, done), timeout=45)
                        self.wakeup.set()
                        while not done.wait(1):
                            if self.stop.is_set():
                                return
                        if stream_id != self.cursor:
                            raise ProtocolError('delivery was not durably applied')
                        backoff = 1
                raise ProtocolError("change stream disconnected")
            except Exception as exc:
                self.errors += 1
                LOG.warning("change stream UNKNOWN; applied cursor retained: %s", exc)
                # No cursor advancement on transport, parser or consumer failure.
                if self.stop.wait(backoff):
                    return
                backoff = min(60, backoff * 2)

    def pump(self, feed, max_pages=BURST_PAGES, budget_s=BURST_SECONDS):
        """Drain a bounded burst, then give adapters/deadlines their reactor turn.

        One queued frame is retained. Briefly wait for the next acknowledged
        frame so a ready backlog does not consume one five-second tick per tx.
        """
        if max_pages <= 0 or budget_s <= 0:
            raise ValueError("stream burst budgets must be positive")
        self.wakeup.clear()
        deadline = time.monotonic() + budget_s
        applied = 0
        for index in range(max_pages):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                if index == 0:
                    stream_id, page, done = self.pending.get_nowait()
                else:
                    stream_id, page, done = self.pending.get(timeout=remaining)
            except queue.Empty:
                break
            try:
                feed.accept_page(page, self.cursor, stream_id=stream_id)
                # Successful return means SQLite committed both inbox and cursor.
                self.cursor = stream_id
                applied += 1
            except Exception as exc:
                feed.errors += 1
                LOG.warning("change delivery UNKNOWN; applied cursor retained: %s", exc)
                return applied
            finally:
                done.set()
        return applied

    def wait(self, timeout):
        """Wake the owner thread when delivery arrives; no graph-read polling."""
        if not self.pending.empty():
            return True
        self.wakeup.clear()
        # A producer can enqueue between the first empty check and clear.
        if not self.pending.empty():
            return True
        return self.wakeup.wait(timeout)

    def close(self):
        self.stop.set()
        self.wakeup.set()
