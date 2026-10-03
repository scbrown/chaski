#!/usr/bin/env python3
"""chaski — a post-commit and time-driven reactor for a quipu knowledge graph.

Stage 1: READ-ONLY. chaski follows quipu's public event feed, evaluates rules,
and exports Prometheus metrics. It writes nothing to quipu and sends no pages.

A rule's `condition` is a SPARQL `SELECT ?focus`. Its result is the set of nodes
the rule is firing for right now; a node leaving the set resolves it.

Three outcomes per evaluation, never two: firing / not firing / UNKNOWN. A query
that times out or errors is UNKNOWN. The rule keeps its previous firing set and
the failure is counted, because rendering "could not look" as "nothing is
firing" silently resolves real alerts.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    import emitter

LOG = logging.getLogger("chaski")

# quipu serves its change feed from the WRITER lock, so a tight poll contends
# with every write in the store. The floor is not configurable downwards.
MIN_EVENT_POLL_S = 60
USER_AGENT = "chaski/0.1"
CLIENT_LABEL = "chaski"

TRIGGER_KINDS = {"event", "schedule"}
SEVERITIES = {"info", "warning", "critical", "page"}
ACTIONS = {"alert", "page", "bead", "webhook"}
DURATION = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")


class RuleError(ValueError):
    pass


def parse_duration(text: str) -> int:
    """ISO-8601 time-only duration (PTnHnMnS) to seconds."""
    m = DURATION.fullmatch(text or "")
    if not m or not any(m.groups()):
        raise RuleError(f"schedule {text!r} is not an ISO-8601 duration like PT5M")
    h, mi, s = (int(g or 0) for g in m.groups())
    return h * 3600 + mi * 60 + s


@dataclass
class Rule:
    """Field names mirror the aegis:Reaction shape (quipu proposal #20) 1:1."""

    label: str
    triggerKind: str
    condition: str
    severity: str
    action: list[str]
    owner: str
    eventTypes: str = ""
    schedule: str = ""
    enabled: bool = True
    comment: str = ""
    interval_s: int = 0

    @classmethod
    def from_dict(cls, d: dict) -> "Rule":
        known = {f for f in cls.__dataclass_fields__ if f != "interval_s"}
        extra = set(d) - known
        if extra:
            raise RuleError(f"unknown field(s) {sorted(extra)}; fields mirror aegis:Reaction")
        missing = [f for f in ("label", "triggerKind", "condition", "severity", "action", "owner") if not d.get(f)]
        if missing:
            raise RuleError(f"rule {d.get('label', '?')!r} is missing {missing}")
        action = d["action"] if isinstance(d["action"], list) else [d["action"]]
        rule = cls(**{**d, "action": action})
        if rule.triggerKind not in TRIGGER_KINDS:
            raise RuleError(f"{rule.label}: triggerKind must be one of {sorted(TRIGGER_KINDS)}")
        if rule.severity not in SEVERITIES:
            raise RuleError(f"{rule.label}: severity must be one of {sorted(SEVERITIES)}")
        if not set(rule.action) <= ACTIONS:
            raise RuleError(f"{rule.label}: action must be within {sorted(ACTIONS)}")
        if "?focus" not in rule.condition:
            raise RuleError(f"{rule.label}: condition must SELECT ?focus")
        if rule.triggerKind == "schedule":
            rule.interval_s = parse_duration(rule.schedule)
        elif not rule.eventTypes:
            raise RuleError(f"{rule.label}: an event rule needs eventTypes")
        return rule


def load_rules(path: Path) -> list[Rule]:
    doc = yaml.safe_load(path.read_text()) or {}
    rules = [Rule.from_dict(r) for r in doc.get("rules", [])]
    labels = [r.label for r in rules]
    dupes = {x for x in labels if labels.count(x) > 1}
    if dupes:
        raise RuleError(f"duplicate rule label(s): {sorted(dupes)}")
    return [r for r in rules if r.enabled]


class Quipu:
    """The public read API only: POST /query and GET /events."""

    def __init__(self, base: str, timeout: float = 10.0):
        self.base = base.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT,
                     "X-Quipu-Client": CLIENT_LABEL},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.load(resp)

    def select_focus(self, sparql: str) -> set[str]:
        """The firing set. Raises on ANY failure: the caller decides UNKNOWN."""
        out = self._request("POST", "/query", {"query": sparql})
        rows = out.get("rows")
        if rows is None:
            raise ValueError(f"no rows in /query response: {str(out)[:200]}")
        return {str(r["focus"]) for r in rows if r.get("focus") is not None}

    def events(self, since: int, types: str, limit: int = 500) -> dict:
        q = urllib.parse.urlencode({"since": since, "types": types, "limit": limit})
        return self._request("GET", f"/events?{q}")


@dataclass
class RuleState:
    firing: set[str] = field(default_factory=set)
    last_success: float = 0.0
    last_attempt: float = 0.0
    evaluations: dict[str, int] = field(default_factory=lambda: {"true": 0, "false": 0, "unknown": 0})
    transitions: int = 0


class Reactor:
    def __init__(self, quipu: Quipu, rules: list[Rule], state_path: Path, event_poll_s: int):
        self.quipu = quipu
        self.rules = rules
        self.state_path = state_path
        self.event_poll_s = max(MIN_EVENT_POLL_S, event_poll_s)
        self.states = {r.label: RuleState() for r in rules}
        self.cursor = self._load_cursor()
        self.event_lag = -1
        self.event_errors = 0
        self.last_event_poll = 0.0
        self.lock = threading.Lock()
        self.change_feed = None
        self.runners: list = []  # stage 2 emitters, attached by main()

    def _load_cursor(self) -> int:
        try:
            return int(json.loads(self.state_path.read_text())["cursor"])
        except (OSError, ValueError, KeyError):
            return -1  # unknown: the first poll asks for the tail and starts there

    def _save_cursor(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"cursor": self.cursor}))
        tmp.replace(self.state_path)

    def evaluate(self, rule: Rule, now: float) -> None:
        st = self.states[rule.label]
        st.last_attempt = now
        try:
            focus = self.quipu.select_focus(rule.condition)
        except Exception as exc:  # noqa: BLE001 — every failure is UNKNOWN by design
            st.evaluations["unknown"] += 1
            LOG.warning("rule %s UNKNOWN (keeping %d firing): %s", rule.label, len(st.firing), exc)
            return
        st.evaluations["true" if focus else "false"] += 1
        started, resolved = focus - st.firing, st.firing - focus
        st.transitions += len(started) + len(resolved)
        for f in sorted(started):
            LOG.info("rule %s FIRING for %s", rule.label, f)
        for f in sorted(resolved):
            LOG.info("rule %s RESOLVED for %s", rule.label, f)
        st.firing = focus
        st.last_success = now

    def poll_events(self, now: float) -> set[str]:
        """Labels of event rules whose types appeared since the cursor."""
        self.last_event_poll = now
        wanted = {t for r in self.rules if r.triggerKind == "event" for t in r.eventTypes.split(",") if t}
        if not wanted:
            return set()
        try:
            if self.cursor < 0:
                out = self.quipu.events(0, ",".join(sorted(wanted)), limit=1)
                # Start at the tail: S1 reacts to what happens from now on, and
                # a replay of history would fire every rule for past events.
                self.cursor = int(out.get("next_offset", 0)) + int(out.get("lag", 0))
                self._save_cursor()
                return set()
            out = self.quipu.events(self.cursor, ",".join(sorted(wanted)))
        except Exception as exc:  # noqa: BLE001
            self.event_errors += 1
            LOG.warning("event poll failed: %s", exc)
            return set()
        seen = {e.get("type") for e in out.get("events", [])}
        self.cursor = int(out.get("next_offset", self.cursor))
        self.event_lag = int(out.get("lag", 0))
        self._save_cursor()
        return {r.label for r in self.rules
                if r.triggerKind == "event" and seen & set(r.eventTypes.split(","))}

    def tick(self, now: float) -> None:
        poll_changes = False
        with self.lock:
            due = {r.label for r in self.rules if r.triggerKind == "schedule"
                   and now - self.states[r.label].last_attempt >= r.interval_s}
            if now - self.last_event_poll >= self.event_poll_s:
                due |= self.poll_events(now)
                poll_changes = self.change_feed is not None
            for rule in self.rules:
                if rule.label in due:
                    self.evaluate(rule, now)
        if poll_changes:
            self.change_feed.poll()
        # OUTSIDE the lock: an adapter run takes minutes, and metrics() takes
        # this lock. Runners publish their own snapshots for the scrape.
        for runner in self.runners:
            if not runner.emitter.change_driven or (self.change_feed and self.change_feed.ready):
                runner.tick(now)

    def metrics(self) -> str:
        with self.lock:
            lines = [
                "# HELP chaski_rule_firing 1 while the rule's condition returns this focus node.",
                "# TYPE chaski_rule_firing gauge",
            ]
            for r in self.rules:
                for f in sorted(self.states[r.label].firing):
                    # keeper = the rule's owner, so Alertmanager routes by who acts on it.
                    lines.append(f'chaski_rule_firing{{rule="{_esc(r.label)}",focus="{_esc(f)}",'
                                 f'severity="{r.severity}",keeper="{_esc(r.owner)}"}} 1')
            lines += ["# HELP chaski_rule_evaluations_total Evaluations by result; unknown = could not look.",
                      "# TYPE chaski_rule_evaluations_total counter"]
            for r in self.rules:
                for res, n in self.states[r.label].evaluations.items():
                    lines.append(f'chaski_rule_evaluations_total{{rule="{_esc(r.label)}",result="{res}"}} {n}')
            lines += ["# HELP chaski_last_success_timestamp_seconds Last evaluation that got an answer.",
                      "# TYPE chaski_last_success_timestamp_seconds gauge"]
            for r in self.rules:
                lines.append(f'chaski_last_success_timestamp_seconds{{rule="{_esc(r.label)}"}} '
                             f'{self.states[r.label].last_success:.0f}')
            lines += ["# TYPE chaski_transitions_total counter"]
            for r in self.rules:
                lines.append(f'chaski_transitions_total{{rule="{_esc(r.label)}"}} {self.states[r.label].transitions}')
            feed_lag = max(self.event_lag, self.change_feed.lag if self.change_feed else -1)
            feed_errors = self.event_errors + (self.change_feed.errors if self.change_feed else 0)
            lines += ["# HELP chaski_events_lag Events behind the feed head at the last poll; -1 = not yet known.",
                      "# TYPE chaski_events_lag gauge", f"chaski_events_lag {feed_lag}",
                      "# TYPE chaski_event_poll_errors_total counter",
                      f"chaski_event_poll_errors_total {feed_errors}"]
            if self.change_feed is not None:
                lines += [f"chaski_changes_lag_transactions {self.change_feed.lag}",
                          f"chaski_changes_errors_total {self.change_feed.errors}"]
            if self.runners:
                lines += ["# HELP chaski_emitter_outbox_pending Events queued and not yet received.",
                          "# TYPE chaski_emitter_outbox_pending gauge"]
                for runner in self.runners:
                    lines += runner.metrics(_esc)
            return "\n".join(lines) + "\n"


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def serve_metrics(reactor: Reactor, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path != "/metrics":
                self.send_error(404)
                return
            body = reactor.metrics().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def load_emitters(path: Path) -> list[tuple[emitter.Emitter, dict]]:
    """Stage 2 emitters (see emitter.py). Separate from `rules` on purpose: a
    rule is a SPARQL condition mirroring aegis:Reaction; an emitter runs an
    external verdict adapter and owns transition state."""
    import emitter

    doc = yaml.safe_load(path.read_text()) or {}
    out = []
    for d in doc.get("emitters", []):
        missing = [f for f in ("label", "command", "schedule", "owner", "sink") if not d.get(f)]
        if missing:
            raise RuleError(f"emitter {d.get('label', '?')!r} is missing {missing}")
        kinds = set(d["sink"])
        if kinds not in ({"jsonl"}, {"graph"}):
            raise RuleError(f"emitter {d['label']}: sink must be {{jsonl: <path>}} or {{graph: {{...}}}}")
        if "graph" in kinds:
            g = d["sink"]["graph"]
            need = [f for f in ("severity", "action", "condition", "comment") if not g.get(f)]
            if need:
                raise RuleError(f"emitter {d['label']}: graph sink needs the Reaction fields {need}")
        e = emitter.Emitter(label=d["label"], command=list(d["command"]),
                            interval_s=parse_duration(d["schedule"]), owner=d["owner"],
                            event=d.get("event", "unblocked"), timeout_s=int(d.get("timeout_s", 300)),
                            key=d.get("key", "item"),
                            from_verdict=d.get("from", emitter.BLOCKED),
                            to_verdict=d.get("to", emitter.UNBLOCKED),
                            baseline_emits=bool(d.get("baseline_emits", False)),
                            change_driven=d.get("trigger", "schedule") == "changes")
        if d.get("trigger", "schedule") not in {"schedule", "changes"}:
            raise RuleError("emitter trigger must be schedule or changes")
        out.append((e, d["sink"]))
    return out


def make_sink(e: emitter.Emitter, sink: dict, quipu_url: str):
    """The receiver for one emitter's events. The graph sink writes, so it is
    the one place chaski needs a quipu write credential: read from a file,
    never from the rules or the command line."""
    import emitter as em

    if "jsonl" in sink:
        return em.JsonlSink(sink["jsonl"])
    import graph_sink

    g = sink["graph"]
    token_file = g.get("token_file")
    token = Path(token_file).read_text().strip() if token_file else None
    reaction = {"label": e.label, "schedule": f"PT{e.interval_s}S", "owner": e.owner,
                "severity": g["severity"], "action": list(g["action"]),
                "condition": g["condition"], "comment": g["comment"]}
    return graph_sink.QuipuFiringSink(quipu_url, token, reaction)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--quipu", required=True, help="quipu base URL")
    ap.add_argument("--rules", required=True, type=Path)
    ap.add_argument("--state", required=True, type=Path, help="event cursor file")
    ap.add_argument("--metrics-port", type=int, default=9481)
    ap.add_argument("--event-poll", type=int, default=MIN_EVENT_POLL_S,
                    help=f"seconds between event polls (floor {MIN_EVENT_POLL_S})")
    ap.add_argument("--once", action="store_true", help="evaluate every rule once, print metrics, exit")
    ap.add_argument("--emitters", type=Path, help="stage 2 emitters YAML (optional)")
    ap.add_argument("--emitter-db", type=Path, help="emitter state (default: beside --state)")
    ap.add_argument("--emitter-write-interval", type=float, default=5.0,
                    help="GLOBAL minimum seconds between sink writes, across all emitters")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    rules = load_rules(args.rules)
    reactor = Reactor(Quipu(args.quipu), rules, args.state, args.event_poll)
    if args.emitters:
        import emitter

        conn = emitter.connect(args.emitter_db or args.state.with_name("emitter.db"))
        budget = emitter.WriteBudget(args.emitter_write_interval)  # ONE, shared by all
        from incremental import ChangeFeed, ChangeRunner

        reactor.runners = [(ChangeRunner if e.change_driven else emitter.Runner)(
            e, conn, make_sink(e, sink, args.quipu), budget) for e, sink in load_emitters(args.emitters)]
        changing = [r for r in reactor.runners if r.emitter.change_driven]
        if changing:
            reactor.change_feed = ChangeFeed(reactor.quipu, changing)
    LOG.info("chaski: %d rule(s), event poll %ds", len(rules), reactor.event_poll_s)
    if args.once:
        now = time.time()
        for rule in rules:
            reactor.evaluate(rule, now)
        print(reactor.metrics(), end="")
        return 0
    serve_metrics(reactor, args.metrics_port)
    while True:
        reactor.tick(time.time())
        time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main())
