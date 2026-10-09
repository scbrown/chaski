# Chaski

Chaski turns changes and elapsed time in a Quipu knowledge graph into durable
reaction events. It reads public event and change feeds, evaluates rules and
adapters, preserves unknown results, and records delivery in a SQLite outbox.

Receivers include JSONL, Quipu reaction firings and configured Alertmanager
routing. A restart resumes from durable state. An event is delivered at least
once; receivers must deduplicate its deterministic ID.

The daemon exposes Prometheus metrics on the configured port. Its canary and
output freshness measure useful work; process health alone does not prove
reaction delivery.
