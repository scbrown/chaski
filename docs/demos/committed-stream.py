import importlib
import json
import queue
import sqlite3
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo))
ChangeStream = importlib.import_module("change_stream").ChangeStream
incremental = importlib.import_module("incremental")
ChangeFeed, SCHEMA = incremental.ChangeFeed, incremental.SCHEMA
conn = sqlite3.connect(":memory:", isolation_level=None)
conn.executescript(SCHEMA)
conn.execute("INSERT INTO change_cursor VALUES(1,0)")
runner = SimpleNamespace(
    conn=conn, emitter=SimpleNamespace(label="seeds"), accepts=lambda r: r["graph"] == "urn:tests:work-board"
)
feed = ChangeFeed(None, [runner])
stream = object.__new__(ChangeStream)
stream.cursor = 0
stream.pending = queue.Queue(maxsize=1)
stream.wakeup = threading.Event()
stop = threading.Event()


def produce():
    for tx in range(1, 14):
        if stop.is_set():
            return
        graph = "urn:tests:work-board" if tx == 13 else "urn:irrelevant"
        page = {
            "records": [
                {
                    "tx": tx,
                    "sequence": 0,
                    "op": "assert",
                    "graph": graph,
                    "entity": "work1",
                    "attribute": "status",
                    "value": "open",
                }
            ],
            "next_tx": tx,
            "watermark_tx": 13,
        }
        done = threading.Event()
        stream.pending.put((tx, page, done))
        stream.wakeup.set()
        while not done.wait(0.01):
            if stop.is_set():
                return


thread = threading.Thread(target=produce)
thread.start()
assert stream.wakeup.wait(1)
print("Before delivery: cursor=0, relevant inbox=0; queue capacity=1")
start = time.monotonic()
turns = 0
while stream.cursor < 13 and time.monotonic() - start < 2:
    stream.wait(0.1)
    stream.pump(feed)
    turns += 1
elapsed = time.monotonic() - start
print(
    json.dumps(
        {
            "probe": "bounded reactor turns, 12 irrelevant transactions before relevant target transaction",
            "pages_applied": conn.execute("SELECT tx FROM change_cursor").fetchone()[0],
            "relevant_deliveries": conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0],
            "elapsed_seconds": elapsed,
            "queue_capacity": stream.pending.maxsize,
            "reactor_turns": turns,
        }
    )
)
stop.set()
try:
    stream.pending.get_nowait()[2].set()
except queue.Empty:
    pass
thread.join(1)
assert not thread.is_alive()

assert stream.cursor == 13
assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone()[0] == 1
