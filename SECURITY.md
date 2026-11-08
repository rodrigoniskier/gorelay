# Security policy

## Report privately

Use GitHub's **Report a vulnerability** mechanism under **Security → Advisories**
for this repository. Do not post secrets, private payloads or exploit-sensitive
information in public issues. No private email address is published as a contact.
The repository maintainer handles reports through GitHub's private advisory
workflow; there is no production support or response-time SLA.

Private vulnerability reporting is a GitHub repository setting, not an application
endpoint. Feature availability and enablement are verified during publication.
If the private reporting form is unavailable, do not disclose sensitive details in
an issue; wait for a private channel to become available. A public bug report may
only describe nonsensitive reproduction/context.

## Local demonstration boundary

GoRelay is an unauthenticated loopback lab. Do not deploy it publicly, tunnel it,
place it behind a public proxy or publish PostgreSQL on wildcard interfaces.
Host/Origin checks are not authentication. Local processes, local recipients,
Docker administrators and operator configuration remain trusted. A loopback
recipient can itself forward traffic; that forwarding is outside this model.

Clients submit opaque destination IDs, not URLs. Operator targets are immutable
literal-loopback HTTP URLs; DNS targets and redirects are rejected, environment
proxies ignored, and request/database contexts bounded. Payload fields use SQL
parameters. These controls do not establish production security or tenant isolation.

Finite attempts can exhaust without delivery. A crash can consume an attempt
before sending; accepted HTTP followed by failed SQL confirmation can duplicate.
Consumers must deduplicate stable GoRelay-Delivery-ID values durably.

## Dependency and disclosure limits

See docs/DEPENDENCIES.md for the prior 25 advisory classifications, the compatible
pgx update, transitive changes, fresh scan result and residual assumptions. Static
symbol reachability is not an exploitation demonstration, and a zero-finding scan
is not proof of absolute safety. Reassess future configuration/feature changes.

Never commit .env, tokens, private keys, database dumps or private logs. CI passwords
and documented review-test credentials are synthetic fixtures, not runtime
credentials. This public snapshot has sanitized fresh history; the original local
engineering/audit history was preserved separately and is not republished.
