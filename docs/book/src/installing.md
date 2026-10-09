# Install and verify

Download the wheel and checksums from a reviewed GitHub release, verify the
wheel's SHA256, then install it with Python 3.10 or newer:

```sh
python3 -m pip install chaski-0.1.0-py3-none-any.whl
# Alternatively:
pipx install ./chaski-0.1.0-py3-none-any.whl
chaski --version
chaski --help
```

The wheel includes the reactor, emitter and receiver modules. PyYAML is its
one dependency and is resolved by the package manager at installation time.

Prove event delivery in a new scratch directory:

```sh
chaski events --directory ./proof
chaski verify-event --directory ./proof --marker local-event-proof
chaski events --directory ./proof
```

The first read must return `[]`. Verification uses the actual emitter, durable
outbox and JSONL receiver: a blocked baseline emits nothing, an unblocked
transition emits one event, and retrying the receiver preserves one event.
The final read returns the marker. Verification refuses an existing proof
database and never contacts a live graph or starts the daemon.

This proves the local event path. Quipu credentials, graph admission, remote
delivery and a configured service's scheduled path require separate checks.
