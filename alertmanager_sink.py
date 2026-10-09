"""Alertmanager sink — deliver an emitter event as an alert, so the event
reaches a person through whatever already routes and escalates alerts.

The graph sink records that a reaction fired. It tells nobody. This sink is the
push half: it POSTs the event to Alertmanager's v2 API, and the deployment's
existing receivers route it from there (an owner label, an escalation chain).
chaski needs no listener of its own on the receiving side.

Identity is the alert's LABEL SET, and Alertmanager deduplicates on it. Every
label is a function of the frozen outbox payload (the emitter label, the event
id, the item), never of the send, so a re-send after a lost acknowledgement
refreshes the same alert instead of raising a second one.

A send is "received" only when Alertmanager says so twice: the POST returns
2xx, and a read-back of the active alerts finds one carrying this event id.
Anything else raises, and the emitter retries the identical send later.

An alert pushed through the API ends at its `endsAt`. This sink sets it to
`ttl_s` after the later of the event and the send, so a send delayed by
backoff still opens a live alert rather than one that is already resolved.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import re
import time
import urllib.parse
import urllib.request

USER_AGENT = "chaski/0.2"
DEFAULT_TTL_S = 24 * 3600
# Alertmanager label values are free text, but the routing labels this sink
# sets are names. Anything outside this set is dropped rather than guessed at.
_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def _iso(ts: float) -> str:
    t = dt.datetime.fromtimestamp(ts, dt.timezone.utc).replace(microsecond=0)
    return t.isoformat().replace("+00:00", "Z")


class AlertmanagerSink:
    """`alert` carries this emitter's alert fields: alertname, severity, and
    optionally ttl_s, summary (a format string over the event payload) and
    extra static labels."""

    def __init__(self, base: str, alert: dict, emitter_label: str,
                 user: str | None = None, password: str | None = None,
                 timeout: float = 15.0, clock=None, owner: str | None = None):
        self.base, self.alert, self.emitter_label = base.rstrip("/"), alert, emitter_label
        # Only deployment configuration may choose routing/severity. An
        # adapter's arbitrary extra fields cannot supply alert policy.
        self.kind_overrides = alert.get("kind_overrides", {})
        if not isinstance(self.kind_overrides, dict):
            raise ValueError("kind_overrides must be a mapping")
        for kind, policy in self.kind_overrides.items():
            if not isinstance(kind, str) or not _NAME.fullmatch(kind):
                raise ValueError("invalid override kind")
            if not isinstance(policy, dict) or not policy or set(policy) - {"severity", "alertname"}:
                raise ValueError("kind override permits only severity and alertname")
            if "severity" in policy and policy["severity"] not in {"info", "warning", "critical"}:
                raise ValueError("invalid override severity")
            if "alertname" in policy and (not isinstance(policy["alertname"], str) or
                                           not _NAME.fullmatch(policy["alertname"])):
                raise ValueError("invalid override alertname")
        # An event without a usable recipient goes to the emitter's owner, so it
        # starts at a person who can act rather than at the admin tier.
        self.owner = owner
        self.ttl_s = int(alert.get("ttl_s", DEFAULT_TTL_S))
        self.timeout = timeout
        self._auth = None
        if user and password:
            self._auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self._clock = clock or time.time

    def _request(self, method: str, path: str, body=None) -> tuple[int, bytes]:
        req = urllib.request.Request(
            self.base + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT,
                     **({"Authorization": self._auth} if self._auth else {})})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.status, resp.read()

    def labels(self, event: dict) -> dict:
        labels = {k: str(v) for k, v in (self.alert.get("labels") or {}).items()}
        labels.update({
            "alertname": self.alert["alertname"],
            "severity": self.alert["severity"],
            "emitter": self.emitter_label,
            "event_id": event["event_id"],
            "item": event["item"],
        })
        kind = event.get("work_kind")
        if isinstance(kind, str) and _NAME.fullmatch(kind):
            labels.update(self.kind_overrides.get(kind, {}))
        for who in (event.get("recipient"), self.owner):
            if who and _NAME.match(str(who)):
                labels["keeper"] = str(who)
                break
        return labels

    def payload(self, event: dict) -> dict:
        start = event["observed_at"]
        end = max(start, self._clock()) + self.ttl_s
        fields = {k: ("" if v is None else v) for k, v in event.items()}
        summary = self.alert.get("summary", "{event} for {item}").format_map(_Missing(fields))
        annotations = {"summary": summary}
        if event.get("evidence"):
            annotations["evidence"] = str(event["evidence"])[:1000]
        return {"labels": self.labels(event), "annotations": annotations,
                "startsAt": _iso(start), "endsAt": _iso(end)}

    def _received(self, event: dict) -> bool:
        q = urllib.parse.urlencode([("filter", f'event_id="{event["event_id"]}"'),
                                    ("filter", f'emitter="{self.emitter_label}"')])
        status, raw = self._request("GET", f"/api/v2/alerts?{q}")
        if status // 100 != 2 or not raw:
            raise ConnectionError(f"read-back: HTTP {status}, {len(raw)} bytes")
        return any(a.get("labels", {}).get("event_id") == event["event_id"] for a in json.loads(raw))

    def deliver(self, event: dict) -> None:
        status, _ = self._request("POST", "/api/v2/alerts", [self.payload(event)])
        if status // 100 != 2:
            raise ConnectionError(f"POST /api/v2/alerts: HTTP {status}")
        if not self._received(event):
            raise ConnectionError(f"{event['event_id']}: accepted but not active on read-back")


class _Missing(dict):
    """str.format_map that leaves an unknown field visible instead of raising."""

    def __missing__(self, key):
        return "{" + key + "}"


class ChainSink:
    """Deliver to each sink in order; received only when ALL have received.

    Order is the point: the graph sink goes first, so a person is told about a
    firing that already exists in the graph. A later failure retries the WHOLE
    chain, which is safe because every sink in it is idempotent on the event
    id; the earlier sinks re-receive what they already hold."""

    def __init__(self, sinks: list):
        self.sinks = sinks

    def setup_pending(self) -> bool:
        return any(getattr(s, "setup_pending", lambda: False)() for s in self.sinks)

    def setup(self) -> None:
        for s in self.sinks:
            if getattr(s, "setup_pending", lambda: False)():
                s.setup()

    def deliver(self, event: dict) -> None:
        for s in self.sinks:
            s.deliver(event)
