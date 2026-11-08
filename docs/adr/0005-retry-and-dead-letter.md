# 0005 — Automatic retry, bounded claims and dead-letter

Status: design recorded before implementation, human acceptance pending.
Base8d0829286a9c055dc3d22b419633d694223c2136, feat/retry-dead-letter.
Migration0004 already exists and stays unchanged; this increment is migration0005.

## Semantics and compatibility

Keep Processor(context.Context,Event)error, stable GoRelay-Delivery-ID, local-only
HTTP, disabled workers and the existing pool. A retry-capable leaseStore adapter
adds Fail(ctx,Lease,error); the pool invokes it only when processing failed and
the parent context remains active. Claim SQL stays centralized in postgresStore.
Generic Stage3A composition and the prior deliveryLeaseStore adapter remain valid;
the normal configured HTTP command uses the new retryDeliveryStore. Old adapters
can still recover by expiry but all central claims have the finite budget guard.
No dashboard, manual re-drive, external service, broker or Stage4.

At-least-once, not exactly-once. A target may accept before SQL confirms, then a
new holder may send the same ID again. Finite attempts/dead-letter do not guarantee
that every event reaches a consumer: crashes before send also consume claim slots.
Consumer deduplication remains necessary. SQL fencing protects state, not effects.

## Policy and scheduling

RetryPolicy {MaxAttempts, BaseDelay, MaxDelay}: defaults5 total committed claims,
1s base,1m cap; max attempts1..100, base1ms..1h, max>=base and<=24h. Runtime variables
GORELAY_MAX_ATTEMPTS, GORELAY_RETRY_BASE_DELAY, GORELAY_RETRY_MAX_DELAY; invalid
configuration fails before startup, never echoes supplied values.

The budget retry_max_attempts is nullable for preexisting/unclaimed rows and is
pinned atomically at first post-upgrade claim/eligible exhaustion maintenance.
Later process configurations do not change an event's pinned budget. Existing
processing_attempts is never reset: older attempts already consumed count. Events
already above a new budget are dead-lettered once eligible without an extra send.
Other timing config applies when each holder records a failure; the committed
next_attempt_at, not recomputation after restart, determines the next eligibility.

For failed attempt n, ceiling=min(MaxDelay,BaseDelay*2^(n-1)) with saturating math.
Equal jitter: delay in[ceiling/2,ceiling], chosen without process-global seed changes.
Pure backoff/decision functions accept an explicit sample for deterministic tests.
For429/503 only, a single Retry-After is parsed as unsigned integer seconds or an
HTTP-date. Reject malformed/negative/overflow/duplicate/overlong data, ignore
past dates, clamp future delay to MaxDelay. Schedule max(jitter_delay,bounded
Retry-After), never exceed cap. No raw header is stored or logged.

Timeout, transient network errors, HTTP408/429 and5xx are retryable. Other non2xx,
including redirects and permanent400/401/403/404, are terminal. Missing destination,
invalid event/configuration are terminal. Unknown processor errors are conservatively
retryable but bounded; classification persists only fixed safe codes and optional
numeric HTTP status, never err.Error(), payload, credentials or URL. Parent shutdown
cancellation performs no failure update: expiry recovery handles the unknown outcome.

## Persistence, transitions and invariants

Migration0005 adds next_attempt_at, retry_max_attempts, last_failure_code,
dead_letter_at and dead_letter_reason. The DLQ is the preserved events row with
status dead_letter and an index, not a copy/move/delete. reason is terminal_failure
or attempts_exhausted. New states: retry_wait and dead_letter; processed/delivered
remain terminal and are never relabeled or reclaimed. Legacy destinationNULL or
fingerprint1 remains untouched/unclaimed, including exhaustion maintenance.

| From | Trigger | To |
| --- | --- | --- |
| pending | Eligible, count<budget, row lock | processing, attempts+1, fresh token |
| retry_wait | next_attempt_at<=DB clock and count<budget | processing, attempts+1 |
| processing | Owned/unexpired success | processed/delivered |
| processing | Owned/unexpired retryable failure, count<budget | retry_wait, clear lease, persist next time |
| processing | Owned/unexpired terminal failure or count>=budget | dead_letter, clear lease |
| processing expired | count<budget | processing with new token, attempts+1 |
| pending/retry_wait/processing expired | count>=budget | dead_letter, no increment/send |

Attempts count committed claims, not confirmed HTTP sends. Claim transaction
atomically updates event counter/token and inserts one event_attempts row before
callback. Unique(event_id,attempt_number) and unique token prevent duplicate slots.
No callback after failed claim commit. No transaction spans HTTP. Completed failure
or success closes the matching in_progress history row in the same fenced
transaction as the event state change; conflicting completion/failure can win once.

History fields: event_id, attempt_number, internal lease_token, started_at,
finished_at, outcome(in_progress,processed,delivered,retry_scheduled,dead_letter,
lease_expired), safe failure_code, optional http_status and scheduled next time.
Pre-upgrade historical sends cannot be reconstructed: no invented old history or
reset counters. Old active leases with no pinned budget/history may complete as
legacy; all new pinned claims require exactly one matching history row to finalize.

Only current token + processing + unexpired database time may Complete/Fail, checked
AFTER locking byID. Stale/expired worker cannot schedule, DLQ, close a newer attempt
or overwrite timestamps. Complete clears next/dead-letter metadata and closes its
history. Fail reads actual counter/budget under lock, not a caller-supplied attempt.
Database wall clock schedules next time; no application clock determines SQL due-ness.

A bounded SKIP LOCKED exhaustion sweep (at most32 per Claim transaction) finalizes
eligible exhausted rows, commits even if no new event can be claimed. Expired
in-progress history closes lease_expired/unknown before replacement/dead-letter.
Recovery after a crash is immediately eligible at lease expiry (already waited a
full lease), without another HTTP-failure backoff; explicit observed HTTP failures
use retry_wait. Final expired attempt goes DLQ rather than receiving a sixth token.

## Crash cases and tests

Before claim commit: no durable slot/callback. After claim commit before/after
send but before result commit: slot remains consumed and in_progress; next recovery
marks lease_expired, rotates token or DLQs at limit. A response/SQL failure may
cause redelivery with identical ID, including terminal responses whose SQL result
was not persisted. Never label unknown crashes as a known HTTP outcome.

Tests force next_attempt_at/expiry only inside isolated SQL schemas to avoid long
sleeps, and separately verify persisted timing bounds using DB timestamps. Required:
503,503,200;429 Retry-After;400 terminal; timeout then success;five failures=>DLQ;
jitter/cap/overflow;restart pending retry;two claimers and failure/complete races;
expired final lease;stale token Fail and Complete;HTTP200/SQL failure;legacy excluded;
upgrade from0004 preserving rows/counts;all prior regressions SQL/race/build.
Independent auditor without source authorship must challenge counts, crashes and
duplication. No test with skipped SQL is approval. Preserve baseline/logs/old files.

References: [Retry-After RFC9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)
and [PostgreSQL SKIP LOCKED](https://www.postgresql.org/docs/17/sql-select.html).
