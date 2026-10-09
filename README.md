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

## Install and first event

Python 3.10 or newer is required. Download a reviewed wheel and its checksums
from [GitHub releases](https://github.com/scbrown/chaski/releases), verify its
SHA256, then install it with pip or pipx:

```sh
pipx install ./chaski-0.1.0-py3-none-any.whl
chaski --version
chaski --help
chaski events --directory ./proof
chaski verify-event --directory ./proof --marker first-event
chaski events --directory ./proof
```

The reads return `[]` before and `["first-event"]` after. The proof runs the
actual adapter, verdict/outbox transaction and JSONL receiver in a new scratch
directory, including a receiver replay control. It never contacts a live graph
or starts the service. Reusing a proof directory is refused.

Run the reactor using explicit configuration and preserved state:

```sh
chaski --quipu http://localhost:3030 --rules rules.example.yaml --state state/cursor.json
```

The source includes durable emitters, graph/JSONL receivers, configured
Alertmanager routing, and change-driven adapters. Sink writes are opt-in through
configuration and share a global write budget. Runtime installation and graph
admission are separate from a successful local event proof.

Read the [installation book](docs/book/src/installing.md),
[configuration](docs/book/src/configuration.md), and
[operations](docs/book/src/operations.md). Contributors should read
[AGENTS.md](AGENTS.md) and [CHANGELOG.md](CHANGELOG.md).

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

### Kind-specific review signals

Verdict adapters may include a bounded `work_kind` token. The emitter freezes it
in the outbox with the event identity, so a retry cannot reclassify an event.
The Alertmanager sink accepts an optional `kind_overrides` map in deployment
configuration. Each exact kind may override only `severity` and `alertname`;
other labels and adapter-supplied severity are ignored. Missing and unknown kinds
retain the emitter's default policy. For example, a deployment can route
`DreamCycle` and `DreamLane` as informational signals while directives remain
warnings. Changing this map is a routing-policy change requiring review.
