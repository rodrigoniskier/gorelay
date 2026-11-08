# Contributing

Describe a concrete issue, reproduction and acceptance criterion. Keep changes
focused, retain the loopback-only trust boundary, and never weaken tests or SQL
integration gates to obtain a green run. Migrations are append-only; do not edit
already-applied files or remove historical data to run tests.

Run sh scripts/verify.sh against a dedicated PostgreSQL database before proposing
a change. Include normal/race results and any unverified scenario. Never submit
credentials, private payloads, raw environment logs or unreviewed screenshots.
Security-sensitive reports belong in GitHub's private vulnerability-reporting
workflow described in SECURITY.md, not a public issue.

MIT covers only original source explicitly listed in LICENSE-SCOPE.json.
Documentation, tests/scripts, configuration, evidence and assets have no automatic
MIT grant; reserved visuals and upstream rights remain separate. Do not introduce
material without the right to submit/distribute it or assume that a contribution
expands the approved license scope. Material contributed through GitHub is also
subject to its applicable terms; any separate licensing agreement must be explicit.
No CLA/DCO or production support promise is asserted here.
