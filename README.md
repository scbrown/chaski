# chaski

A post-commit and time-driven **reactor** for a [quipu](https://github.com/scbrown/quipu)
knowledge graph.

In the Inca empire, chaski were the relay runners who carried quipus between
stations. This one carries what the graph learned to the people and systems who
need to act on it.

## What it does

quipu records facts. chaski watches for facts that call for action, and acts:

- **Event rules** fire when a write changes something that matters. For example,
  "a credential was verified and is now dead". chaski follows quipu's durable
  event feed (`GET /events` with a committed consumer offset), so a restart
  resumes exactly where it stopped.
- **Schedule rules** fire when time makes something true. For example, "a
  credential expires within 14 days". quipu has no timers of its own; chaski is
  the scheduler.

Rules are data. Each rule is a SPARQL `SELECT ?focus` whose result is the set of
nodes the rule is firing for **right now**. A node leaving the set resolves the
alert, so alerts resolve on their own.

## Design constraints

- **Public API only.** chaski never reads quipu's storage. A reactor that reads
  a store's internals dies the day the store changes.
- **Liveness is measured by output, not process.** A canary rule must fire every
  cycle. A chaski that is running but not evaluating alerts on itself.
- **Unknown is not "all clear".** A query that times out is counted as
  `unknown`, never as "not firing".
- **Writes are transitions only.** Steady state writes nothing to the graph.
- **Emission reuses Prometheus and Alertmanager.** chaski exports metrics; it
  does not invent notification plumbing.

## Status

Stage 1: a read-only skeleton, with the event tail, schedule rules, metrics and
the canary. No writes and no pages yet.

Stage 2 (in progress): **emitters** (`emitter.py`). An emitter runs an external
verdict adapter on a schedule and turns its answers into transition events. A
first sighting is a baseline, "unknown" keeps the last verdict, and only a
known blocked-to-unblocked transition emits. Verdict and outbox commit in one
transaction. Delivery is at least once to a receiver that deduplicates on the
adapter's deterministic event id. Configure with `--emitters emitters.yaml`:

```yaml
emitters:
  - label: workitem-unblocked
    command: [python3, /path/to/blocked_by.py]
    schedule: PT15M
    owner: someone
    event: unblocked
    sink: {jsonl: /var/lib/chaski/unblocked.jsonl}
```

## License

Apache-2.0. See [LICENSE](LICENSE).

### Incremental verdict adapters

An emitter can set `trigger: changes` and `schedule: PT24H`. The schedule then
means catalogue reconciliation, not a full verdict scan. The adapter must support
`--describe` (local subscription metadata) and `--changes` (JSON on stdin). Existing
scheduled adapters remain supported.

The version-1 protocol describes `attributes`, `types` and trusted `graphs`.
A request contains `now` plus one of:

- `discover: true`: return `items`, the complete catalogue of candidate keys.
- `route: [...]`: return `items` affected by the supplied fact changes. Retracted
  links carry `old_value`; adapters must include their former owners.
- `items: [...]`: return `scope`, `records` and `next_checks` (one future Unix
  deadline or null per record). Missing keys inside scope become untracked;
  keys outside scope retain their verdict. UNKNOWN preserves the last verdict.

Every response includes `version: 1`. The first discovery starts after a durable
feed-tail checkpoint; subsequent changes cannot fall through the bootstrap gap.
Chaski polls `/changes` with `old_and_new_values` at the existing 60-second floor,
committing the cursor and matching inbox records together. Routing atomically
moves inbox records into a persistent key queue. Two changed subjects and two
ready keys are processed per tick, with changed keys ahead of reconciliation.
Verdicts, future deadlines and transition outbox records commit together. A
failed operation leaves its work for retry, and restart does not repeat discovery.
The existing idempotent receiver and global write budget still govern delivery.

`chaski_changes_lag_transactions`, `chaski_changes_errors_total` and per-emitter
`changes_pending`, `deadlines`, `next_due_timestamp_seconds` expose feed and queue
health. The existing event lag/error signals also include the change feed. Idle
adapters make no queries until a matching change, deadline or reconciliation.
Catch-up is bounded to ten pages of 100 transactions per poll; sustained overload
is visible as lag rather than silently dropping work. Latency includes queueing
and adapter runtime; a large dependency fan-out can take multiple ticks.

Adapter state uses additive SQLite tables in the existing emitter database.
Rollback to a scheduled adapter preserves verdicts and delivered-event identities;
keep that database, and restore the previous code and emitter configuration.

### Authenticated external event sources

An authenticated webhook or local event source can queue an explicit event into
Chaski's existing emitter outbox with `external_event.py`. Authentication belongs
to that source; this local CLI exposes no network listener. Configure an external
emitter with `baseline_emits: true` so the first published event is delivered:

```yaml
emitters:
  - label: release-pin
    trigger: external
    schedule: PT24H # metadata only; no periodic verdict adapter is invoked
    owner: kit
    event: release.published
    from: ABSENT
    to: PUBLISHED
    baseline_emits: true
    sink: {jsonl: /var/lib/example/releases.jsonl}
```

```sh
printf '%s\n' '{"event_id":"repo-kit-1","type":"release.published","payload":{"tag":"v1.2.3"}}' |
  python external_event.py --emitters emitters.yaml --label release-pin --state emitter.db
```

Use the same emitter database and configuration as the running Chaski instance.
The existing runner delivers pending events on its next tick without running a
verdict adapter or scanning remote data. Existing graph, command, alert, retry,
write-budget and receiver-deduplication contracts remain in force. A command
receiver must return nonzero while held or on failed proof: exit zero is its
receipt and acknowledges delivery.

The receipt contains the original source ID only after a synchronous transaction
proves the exact outbox payload. A lost receipt may be retried with identical
bytes; restart and redelivery do not queue a second firing. Changed payload under
an existing source ID refuses. The outbox ID and graph focus use a hash scoped to
the emitter, so arbitrary source identifiers cannot become graph syntax and two
emitters can consume the same source event independently. Graph reaction metadata
records an event trigger for this mode. No webhook or installer is activated by
adding the source code.
