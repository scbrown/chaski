# Operations and releases

Monitor the canary, event/change-feed lag, unknown evaluations, pending outbox
age and receiver errors. A running process with stale output is not healthy.

Upgrade by staging a reviewed checksummed wheel into a new environment and
proving its version and isolated event round trip. Switch the service only
after configuration and graph prerequisites pass. Preserve state across the
switch. Roll back to the previous environment and configuration, retaining
the same durable state and delivery IDs.

The tag release workflow requires the tag to match the package version, runs
the full tests and lint, builds wheel and source artifacts with a fixed build
timestamp, installs and exercises the wheel, and publishes SHA256 checksums.
Publication, installation and observed service execution are separate steps.
