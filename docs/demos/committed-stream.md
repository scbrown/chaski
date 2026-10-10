# Durable bounded delivery demo

Isolated in-memory SQLite fixture and controlled producer, not an installed reactor.
Base: 3680143b898e71628100fc3ecd524eb816f6b3d8. Runtime source:
bbf60a0f0351fc967fbad9cd4be189a8a15cae70; this demo-only commit leaves runtime unchanged.

Run `python3 docs/demos/committed-stream.py`. Cursor/inbox start at zero. Twelve
unrelated transactions precede one subscribed transaction: the durable cursor
reaches 13 and the relevant inbox count reaches 1, while queue capacity stays 1.
Each pump retains the production 64-page/50ms budget; completion may span turns.
An individual commit can exceed that budget. This is not a deployed latency claim.
The base has polling only and cannot run this new stream reproducer.

Download [output](committed-stream.txt) and [timing](committed-stream.time), then run
`scriptreplay --log-timing committed-stream.time --log-out committed-stream.txt`.
Playback was verified locally. These small sanitized artifacts are retained in Git.
