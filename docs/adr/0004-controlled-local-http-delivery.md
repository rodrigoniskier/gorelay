# 0004 — Stage3B-1: controlled local HTTP delivery

Status: contract recorded before implementation; human acceptance pending.
Required base3f56339223033ddd441f991a6581975863f26299, feat/http-delivery.

## Contract and destination policy

Reuse Processor(context.Context,Event)error and the existing WorkerPool/Claim;
no duplicate claim loop or change to ownership tokens. A configured destination
maps an immutable case-sensitive destination_id to one explicit URL, set only by
the operator, never by an event client. The Stage2 POST fields/fingerprint and
201/200/409 remain unchanged. Unknown destination fails processing without losing
or inventing event information. No resolver, dashboard or complete retry policy.

Only a demonstration HTTP target on a literal loopback IP, with an explicit
nonzero numeric port, is accepted. Reject HTTPS/non-HTTP schemes for this local
increment, DNS hostnames (including localhost), wildcard/non-loopback addresses,
userinfo/credentials, query/fragment and malformed URLs. Map size1..16, copied on
construction. A guarded net.Dialer rechecks loopback at connection time; no DNS,
proxy from environment, redirects or public endpoint is used. Tests use isolated
httptest HTTP servers bound to127.0.0.1 and PostgreSQL schemas; no external calls.
This is local demo scope, not a general SSRF solution or production egress policy.

HTTP request: POST, Content-Type application/json, stable header
`GoRelay-Delivery-ID: <event UUID>` on every attempt and every process. Envelope:
`id`, `destination_id`, `type`, `payload`, `created_at` (UTC). Do not send the
idempotency key, lease token, attempts or transient processing status. The consumer
must use this stable ID for durable deduplication; GoRelay does not force its
semantics. Do not call the header Idempotency-Key, avoiding Transport's special
idempotent-retry treatment for that header on POST.

## Outcomes and SQL confirmation

2xx means the HTTP processor reports success. The response body is not required,
is not logged, and is closed rather than drained without bound. Non2xx (including
redirect), deadline, cancellation, dial or protocol error returns a typed/safe
error category/status without URL/body/payload/credential text. HTTP success alone
never changes SQL. WorkerPool invokes a deliveryLeaseStore adapter which shares
Claim and delegates completion to the same fenced code as Stage3A.

Migration0004 adds delivered_at and extends terminal state to delivered. Generic
Stage3A Complete remains processed; CompleteDelivery uses the same ID row lock,
token, processing state and unexpired database-time checks, then commits status
`delivered`, processed_at and delivered_at together, clearing lease fields.
processed rows already present stay processed, never retroactively delivered or
reclaimed. delivered rows are terminal and never eligible for Claim. Existing
migrations and their checksums are preserved. Readiness checks the new column.
Only state/timestamp of confirmed delivery is recorded; individual HTTP response
codes/bodies are not retained in this bounded increment.

## Timing, lifecycle and explicit activation

http.Client has total request timeout, guarded connection timeout and redirects
disabled (ErrUseLastResponse). Require connect_timeout<=request_timeout and
request_timeout+2*worker_DB_timeout<lease_duration. The two DB budgets cover claim
commit latency and final confirmation; remaining time is safety margin. Database
clock and fencing stay authoritative if scheduling/network latency exceeds these
bounds. No heartbeat or lease renewal is added. Processor context derives from
worker context; canceled callbacks must not return success or cause confirmation.

Defaults: GORELAY_WORKERS=0, GORELAY_HTTP_TIMEOUT=5s (max1m),
GORELAY_HTTP_CONNECT_TIMEOUT=1s. Workers from the normal command can activate HTTP
only with GORELAY_WORKERS>0 plus GORELAY_HTTP_DEMO=1 and GORELAY_DESTINATIONS JSON
map. Example map: {"demo":"http://127.0.0.1:9091/events"}. Any missing/invalid
configuration fails before DB/listener startup. Disabled workers do not open HTTP
connections or require destination configuration. Existing runWithProcessor Go
composition still uses generic processed completion to preserve Stage3A tests;
normal run selects the HTTP adapter only for explicit local demo activation.
Client idle connections close after the pool stops. API remains literal loopback,
Host constrained, Origin rejected, no public bind override.

## At-least-once and failure window

If the receiver accepts a POST but SQL confirmation fails or the lease is lost,
the row remains processing until expiry. A new holder may send the same event
again with the same delivery ID. Even cancellation/timeout can occur after the
receiver accepted a body. There is no exactly-once promise or distributed atomic
transaction between HTTP and PostgreSQL. Fencing rejects stale SQL confirmation,
not already-issued HTTP side effects. Consumer deduplication must span restarts.
Repeated attempts after lease expiry use existing Stage3A behavior, not a new
backoff, dead-letter or retry scheduler. Legacy unknown destinations stay excluded.

## Required verification and limits

Positive local POST200/2xx and delivered SQL;500 preserves row; timeout/cancel/
connection failure; missing/unknown destination; URL/arbitrary endpoint rejection;
redirect not followed (including remote Location without making a remote call);
proxy ignored; two pool instances/one event; late2xx after expiry rejects completion;
real SQL failure after200 followed by duplicate with same ID; prior processed rows
preserved and Stage2/3A regression suite including subprocess restart. Required
PostgreSQL, normal/race tests and build; independent auditor must try concurrency,
duplication and response/SQL failure window in an isolated copy without source edits.
No claims about production TLS/auth/egress, full retry policy, network partitions,
physical faults, long soak or remote CI without execution.

References: [http.Client](https://pkg.go.dev/net/http#Client),
[http.Transport](https://pkg.go.dev/net/http#Transport),
[net.Dialer](https://pkg.go.dev/net#Dialer).
