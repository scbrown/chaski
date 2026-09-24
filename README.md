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

## License

Apache-2.0. See [LICENSE](LICENSE).
