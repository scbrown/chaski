# Chaski contributor instructions

Chaski supports Python 3.10 and newer. Keep the service argument contract and
flat runtime modules compatible with the installed reactor.

Run the full pytest suite and Ruff before proposing a change. Build a wheel
with `uv build`, then run `python scripts/smoke_wheel.py dist/*.whl` outside an
active service installation. The smoke command creates and removes its own
temporary environment; it never talks to a live graph.

Runtime state includes event cursors, verdicts, deadlines and the outbox. Do
not delete or reset it to make a check pass. Unknown adapter results must
preserve the previous verdict. A successful delivery requires receiver proof.

Release from a reviewed main commit with a `v<version>` tag matching
`chaski_version.py`. The release workflow tests the built wheel before
publishing its wheel, source distribution and checksums. Keep credentials out
of source, logs and example configuration. Release artifacts do not establish
that any service installed or ran them; verify deployment separately.
