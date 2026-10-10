import io
import json
import queue
import threading
from types import SimpleNamespace

import pytest

from change_stream import ChangeStream, FRAME_BYTES, frames
from emitter import ProtocolError


def frame(cursor=11, page=None):
    page = page or {'records': [], 'next_tx': cursor, 'watermark_tx': cursor}
    return f'event: quipu.changes\nid: {cursor}\ndata: {json.dumps(page)}\n\n'.encode()


def test_heartbeats_and_transaction_ids():
    expected = [(11, {'records': [], 'next_tx': 11, 'watermark_tx': 11})]
    assert list(frames(io.BytesIO(b': keepalive\n\n'+frame()))) == expected


@pytest.mark.parametrize('raw', [frame()[:-1], frame().replace(b'id: 11',b'id: -1'),
    frame().replace(b'id: 11',b'id: 9223372036854775808'),
    frame().replace(b'quipu.changes',b'quipu.error'),
    frame().replace(b'id: 11',b'id: 11\nid: 12'), b'data: '+b'x'*FRAME_BYTES+b'\n\n'])
def test_invalid_or_partial_frame_refuses(raw):
    with pytest.raises((ProtocolError, ValueError)):
        list(frames(io.BytesIO(raw)))


def test_delivery_failure_reconnects_from_applied_cursor_only():
    stream = object.__new__(ChangeStream)
    stream.cursor = 10
    stream.wakeup = threading.Event()
    stream.pending = queue.Queue(maxsize=1)
    done = threading.Event()
    stream.pending.put((11, {'next_tx': 11}, done))
    def refuse(*args, **kwargs):
        raise RuntimeError('commit refused')
    feed = SimpleNamespace(accept_page=refuse, errors=0)
    stream.pump(feed)
    assert stream.cursor == 10 and done.is_set() and feed.errors == 1
    done = threading.Event()
    stream.pending.put((11, {'next_tx': 11}, done))
    applied = []
    feed.accept_page = lambda page, since, stream_id: applied.append((since,stream_id))
    stream.pump(feed)
    assert applied == [(10,11)] and stream.cursor == 11 and done.is_set()


def test_stream_reactor_idle_never_polls_change_feed(tmp_path):
    from chaski import Reactor
    q = SimpleNamespace()
    reactor = Reactor(q, [], tmp_path / 'cursor.json', 60)
    def forbidden():
        raise AssertionError('idle stream must not poll graph metadata')
    reactor.change_feed = SimpleNamespace(poll=forbidden, ready=True)
    pumped = []
    reactor.change_stream = SimpleNamespace(pump=lambda feed: pumped.append(feed))
    for now in range(60, 7201, 60):
        reactor.tick(now)
    assert len(pumped) == 120


def queued_stream(count):
    stream = object.__new__(ChangeStream)
    stream.cursor = 0
    stream.pending = queue.Queue(maxsize=1)
    stream.wakeup = threading.Event()
    halt = threading.Event()
    def produce():
        for cursor in range(1, count + 1):
            if halt.is_set():
                return
            done = threading.Event()
            stream.pending.put((cursor, {'next_tx': cursor}, done))
            stream.wakeup.set()
            while not done.wait(.01):
                if halt.is_set():
                    return
    thread = threading.Thread(target=produce)
    thread.start()
    assert stream.wakeup.wait(1)
    def close():
        halt.set()
        try:
            stream.pending.get_nowait()[2].set()
        except queue.Empty:
            pass
        thread.join(1)
        assert not thread.is_alive()
    return stream, close


def test_burst_page_budget_yields_and_wait_wakes_for_remainder():
    stream, close = queued_stream(13)
    applied = []
    feed = SimpleNamespace(accept_page=lambda page, since, stream_id: applied.append(stream_id), errors=0)
    try:
        assert stream.pump(feed, max_pages=3, budget_s=1) == 3
        assert stream.cursor == 3 and applied == [1, 2, 3]
        assert stream.wait(1)
        assert stream.pump(feed, max_pages=10, budget_s=1) == 10
        assert stream.cursor == 13 and applied == list(range(1, 14))
        assert stream.pending.maxsize == 1
    finally:
        close()


def test_burst_time_budget_yields_between_commits(monkeypatch):
    import change_stream
    stream, close = queued_stream(13)
    clock = iter([0.0, 0.0, 0.06])
    monkeypatch.setattr(change_stream.time, 'monotonic', lambda: next(clock))
    feed = SimpleNamespace(accept_page=lambda *a, **kw: None, errors=0)
    try:
        assert stream.pump(feed, max_pages=64, budget_s=.05) == 1
        assert stream.cursor == 1
    finally:
        close()


def test_delivery_ready_wakes_without_a_five_second_wait():
    stream = object.__new__(ChangeStream)
    stream.wakeup = threading.Event()
    stream.pending = queue.Queue(maxsize=1)
    stream.pending.put((11, {}, threading.Event()))
    stream.wakeup.set()
    assert stream.wait(5)
