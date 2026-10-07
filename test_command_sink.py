"""Command sink: a real subprocess, no fakes of subprocess itself."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

import command_sink as cs

EVENT = {"event_id": "ev-1", "item": "item-1", "event": "untraced", "observed_at": 1.0}


def script(body: str) -> list[str]:
    return [sys.executable, "-c", body]


class Delivery(unittest.TestCase):
    def test_exit_zero_is_the_receipt_and_the_event_arrives_on_stdin(self):
        out = Path(tempfile.mkdtemp()) / "got.json"
        cs.CommandSink(script(f"import sys; open({str(out)!r},'w').write(sys.stdin.read())")).deliver(EVENT)
        self.assertEqual(json.loads(out.read_text()), EVENT)

    def test_non_zero_exit_raises_with_its_stderr(self):
        with self.assertRaises(ConnectionError) as ctx:
            cs.CommandSink(script("import sys; sys.stderr.write('quipu 502'); sys.exit(3)")).deliver(EVENT)
        self.assertIn("exited 3", str(ctx.exception))
        self.assertIn("quipu 502", str(ctx.exception))

    def test_timeout_and_missing_program_raise(self):
        with self.assertRaises(ConnectionError):
            cs.CommandSink(script("import time; time.sleep(5)"), timeout_s=0.5).deliver(EVENT)
        with self.assertRaises(ConnectionError):
            cs.CommandSink(["/nonexistent/proposer"]).deliver(EVENT)

    def test_empty_argv_is_refused(self):
        with self.assertRaises(ValueError):
            cs.CommandSink([])


class Wiring(unittest.TestCase):
    GRAPH = {"severity": "info", "action": ["alert"], "condition": "SELECT ?focus WHERE {}", "comment": "c"}

    def load(self, sink):
        import chaski
        d = Path(tempfile.mkdtemp())
        cfg = d / "e.yaml"
        cfg.write_text(json.dumps({"emitters": [{"label": "directive-untraced", "owner": "ian",
                                                 "schedule": "PT1M", "command": ["true"], "sink": sink}]}))
        return chaski, chaski.load_emitters(cfg)

    def test_graph_then_command_then_alertmanager(self):
        import alertmanager_sink as am
        import graph_sink as gs
        chaski, ((e, sink),) = self.load({"graph": self.GRAPH, "command": {"argv": ["true"]},
                                          "alertmanager": {"url": "http://am.invalid", "alertname": "X",
                                                           "severity": "warning"}})
        built = chaski.make_sink(e, sink, "http://q")
        self.assertEqual([type(s) for s in built.sinks],
                         [gs.QuipuFiringSink, cs.CommandSink, am.AlertmanagerSink])

    def test_graph_then_command_alone(self):
        chaski, ((e, sink),) = self.load({"graph": self.GRAPH, "command": {"argv": ["true"], "timeout_s": 30}})
        built = chaski.make_sink(e, sink, "http://q")
        self.assertEqual(built.sinks[1].timeout_s, 30.0)

    def test_command_without_graph_or_argv_is_refused(self):
        import chaski
        for sink in ({"command": {"argv": ["true"]}}, {"graph": self.GRAPH, "command": {}}):
            with self.assertRaises(chaski.RuleError):
                self.load(sink)


if __name__ == "__main__":
    unittest.main()
