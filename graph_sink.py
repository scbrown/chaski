"""Graph sink — deliver an emitter event as an aegis:ReactionFiring in quipu.

The receiver IS the graph, and it deduplicates because the firing's identity is
a function of (reaction label, event id), never of the send:

    firing IRI = <ns>firing-<sha256(json([label, event_id]))[:32]>

and the write is a `/knot` with `replace_snapshot` keyed on that IRI, built only
from the FROZEN outbox payload (`startedAt` is the payload's `observed_at`, not
the send time). A re-send after a lost acknowledgement therefore rewrites the
same bytes under the same key: one firing, however many sends.

A write is "received" only when the graph says so twice: the `/knot` response
carries a transaction id and conforms, and a read-back finds the firing's
eventId. A timeout or an empty body is NOT a failed write (it may have landed),
so it raises and the emitter retries the identical write later, which is safe
exactly because the write is idempotent.

The Reaction the firings point at is written once per sink (its own snapshot),
because the shape requires `firedBy` to be a typed Reaction.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import socket
import urllib.request

NS = "http://aegis.gastown.local/ontology/"
# The quechua twin of the TERMS this sink reads (aegis-9dpcta). Only terms move:
# firing, reaction and focus IRIs stay under NS, because instance identity does
# not change in the transition. The served quipu does not honour
# owl:equivalentClass on reads, so the read-back names both term IRIs itself.
QUECHUA_NS = "https://scbrown.github.io/quechua/ns#"
USER_AGENT = "chaski/0.2"
# A caller KIND of its own, so the emitter's writes are separable from chaski's
# rule reads in quipu's per-caller accounting.
CLIENT_LABEL = "chaski-emitter"


_UNSAFE_HEADER = re.compile(r"[^\x21-\x7e ]")


def provenance_headers(env=None) -> dict:
    """Structured write provenance (aegis-7zp4rc): chaski is a service, so it
    names itself (agent=chaski, harness=service, host); QUIPU_AGENT /
    QUIPU_HARNESS / QUIPU_HOST override. No session or model: a service has
    neither, and quipu's coverage metric does not expect them. Values are
    single-line printable ASCII, capped at 128 characters."""
    env = os.environ if env is None else env
    raw = {"Agent": env.get("QUIPU_AGENT") or "chaski",
           "Harness": env.get("QUIPU_HARNESS") or "service",
           "Host": env.get("QUIPU_HOST") or socket.gethostname()}
    out = {}
    for field, value in raw.items():
        text = _UNSAFE_HEADER.sub("", str(value)).strip()[:128]
        if text:
            out[f"X-Quipu-{field}"] = text
    return out


def firing_iri(label: str, event_id: str, ns: str = NS) -> str:
    key = json.dumps([label, event_id], separators=(",", ":"))
    return f"{ns}firing-{hashlib.sha256(key.encode()).hexdigest()[:32]}"


def _lit(s: str) -> str:
    return json.dumps(s)  # a valid Turtle string literal: quotes and escapes


class QuipuFiringSink:
    """`reaction` carries the aegis:Reaction fields for this emitter:
    label, schedule, condition, severity, action (list), owner, comment."""

    def __init__(self, base: str, token: str | None, reaction: dict,
                 ns: str = NS, timeout: float = 30.0):
        self.base, self.token, self.reaction, self.ns, self.timeout = base.rstrip("/"), token, reaction, ns, timeout
        self._reaction_written = False

    def _post(self, path: str, body: dict) -> dict:
        req = urllib.request.Request(
            self.base + path, data=json.dumps(body).encode(), method="POST",
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT,
                     "X-Quipu-Client": CLIENT_LABEL, **provenance_headers(),
                     **({"Authorization": f"Bearer {self.token}"} if self.token else {})})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            raw = resp.read()
        if not raw:
            raise ConnectionError(f"{path}: empty response (the write may have landed)")
        return json.loads(raw)

    def _knot(self, turtle: str, snapshot: str) -> None:
        out = self._post("/knot", {"turtle": turtle, "actor": "chaski", "source": snapshot,
                                   "replace_snapshot": True, "snapshot": snapshot})
        if out.get("conforms") is False:
            raise ValueError(f"quipu refused {snapshot}: {str(out)[:300]}")
        if not out.get("tx_id"):
            raise ConnectionError(f"{snapshot}: no tx_id in {str(out)[:200]} (indeterminate)")

    def reaction_iri(self) -> str:
        return f"{self.ns}reaction-{self.reaction['label']}"

    def reaction_turtle(self) -> str:
        r = self.reaction
        actions = ", ".join(_lit(a) for a in r["action"])
        return (
            f"<{self.reaction_iri()}> a <{self.ns}Reaction> ;\n"
            f"  <http://www.w3.org/2000/01/rdf-schema#label> {_lit(r['label'])} ;\n"
            f"  <{self.ns}triggerKind> \"schedule\" ; <{self.ns}schedule> {_lit(r['schedule'])} ;\n"
            f"  <{self.ns}condition> {_lit(r['condition'])} ;\n"
            f"  <{self.ns}severity> {_lit(r['severity'])} ; <{self.ns}action> {actions} ;\n"
            f"  <{self.ns}owner> {_lit(r['owner'])} ;\n"
            f"  <http://www.w3.org/2000/01/rdf-schema#comment> {_lit(r['comment'])} .\n"
        )

    def firing_turtle(self, event: dict) -> str:
        started = dt.datetime.fromtimestamp(event["observed_at"], dt.timezone.utc)
        started = started.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        focus = event["item"] if "://" in event["item"] else f"{self.ns}{event['item']}"
        return (
            f"<{firing_iri(self.reaction['label'], event['event_id'], self.ns)}> a <{self.ns}ReactionFiring> ;\n"
            f"  <{self.ns}firedBy> <{self.reaction_iri()}> ;\n"
            f"  <{self.ns}focus> <{focus}> ;\n"
            f"  <{self.ns}startedAt> \"{started}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;\n"
            f"  <{self.ns}eventId> {_lit(event['event_id'])} .\n"
        )

    def _received(self, event: dict) -> bool:
        iri = firing_iri(self.reaction["label"], event["event_id"], self.ns)
        # Legacy OR quechua eventId, as a UNION of two single bound patterns (one
        # request). Not a (p1|p2) path: the served quipu drops literal objects
        # there (aegis-sxlptn), and eventId is a literal.
        q = (f"SELECT ?e WHERE {{ {{ <{iri}> <{self.ns}eventId> ?e }} UNION "
             f"{{ <{iri}> <{QUECHUA_NS}eventId> ?e }} }}")
        out = self._post("/query", {"query": q})
        rows = out.get("rows")
        if rows is None:
            raise ConnectionError(f"read-back returned no rows field: {str(out)[:200]}")
        return any(str(r.get("e")) == event["event_id"] for r in rows)

    def setup_pending(self) -> bool:
        """The Reaction every firing points at is owed once per process. The
        emitter spends a write-budget slot on it, separately from any event."""
        return not self._reaction_written

    def setup(self) -> None:
        self._knot(self.reaction_turtle(), f"chaski:reaction:{self.reaction['label']}")
        self._reaction_written = True

    def deliver(self, event: dict) -> None:
        if not self._reaction_written:
            # Called outside deliver_pending (a direct caller): still correct,
            # just not budgeted separately.
            self.setup()
        iri = firing_iri(self.reaction["label"], event["event_id"], self.ns)
        self._knot(self.firing_turtle(event), iri)
        if not self._received(event):
            raise ConnectionError(f"{iri}: written but not found on read-back (indeterminate)")
