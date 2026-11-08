# 0002 — Destination identity, idempotency and local scope

Status: implemented for local review; human acceptance pending, no commit yet.

The consolidation requires an opaque `destination_id`, as confirmed by the user.
The accepted request has three semantic fields: destination_id, type and payload.
The v2 SHA-256 fingerprint includes all three plus `gorelay:fingerprint:v2:POST:/v1/events`.
Object ordering/whitespace are normalized; numeric lexemes remain significant.
A global key owns one immutable event: 201 on creation, 200 same fingerprint,
409 different fingerprint. No worker, endpoint resolver or destination registry.

0001 remains byte-identical. The additive 0002 preserves legacy rows, timestamps
and hashes. destination_id is NULL and fingerprint_version=1 for those rows.
New events require destination_id and store version=2. The destination CHECK is
NOT VALID only to retain legacy data; PostgreSQL still enforces it on new writes.
An old key cannot be silently reassigned to a destination: a v2 request conflicts.
This intentionally avoids fabricating delivery targets or rehashing JSONB values
whose original numeric lexemes are no longer available. A reviewed backfill and
future migration are necessary before legacy delivery or updates.

Without authentication the service is local-only: literal loopback bind, strict
Host and no Origin-bearing requests. No insecure wildcard override. Linux Docker
uses host networking with loopback; bridged port publishing is unsupported.
These controls do not stop local processes, proxies, port forwarding or tunnels;
operators must not expose the listener. Database credentials remain server-side.

CI uses PostgreSQL 17 with a health check and required integration mode, normal
and race runs via scripts/verify.sh. Missing DSN or unavailable database fails;
optional local unit runs may skip only outside required mode. No remote CI run
is asserted without actual execution.

References: [PostgreSQL NOT VALID semantics](https://www.postgresql.org/docs/17/sql-altertable.html)
and [GitHub PostgreSQL service containers](https://docs.github.com/en/actions/tutorials/use-containerized-services/create-postgresql-service-containers).
