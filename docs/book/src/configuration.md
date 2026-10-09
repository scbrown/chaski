# Configuration and state

Run the reactor with explicit configuration and state paths:

```sh
chaski --quipu http://localhost:3030 --rules rules.yaml --state state/cursor.json
```

Rules use `triggerKind`, `condition`, `severity`, `action`, `owner` and optional
schedule/event filters. The condition returns `?focus`. Unknown evaluations
retain the last firing set rather than resolving it.

Optional emitters run verdict adapters. Each emitter declares its command,
owner, transition and sink. Change-driven adapters receive discovery, routing
and bounded evaluation requests; their daily schedule reconciles catalogue
state. Event polling retains the sixty-second floor and sink writes share one
global budget.

Keep cursor files, emitter databases, verdicts, deadlines and delivery history.
Credentials are read from explicitly configured files; do not put them in
rules, package metadata or command arguments.
