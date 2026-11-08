# 0003 — Stage 3A: concurrent processing with fenced leases

Status: design recorded before implementation; local review, human acceptance pending.
Base: 548c523a2580d614b48fb7cc66d1ef8ed149101b.

## Scope and semantics

Implement a bounded Go worker engine with an injected Processor callback and a
PostgreSQL lease store. No HTTP delivery, destination resolution, full retry
policy, heartbeat, dashboard or external broker. Intended semantics are
**at-least-once**: a callback may run again after a crash or expired lease. The
callback must eventually be idempotent when it gains side effects. We do not
claim exactly-once. Database fencing protects state changes, not external effects.

No built-in no-op processor may mark real events as delivered. `processed` means
only that the explicitly injected callback returned success and the holder
committed its confirmation; it never means HTTP delivery. The normal binary has
no delivery callback and leaves workers disabled by default (GORELAY_WORKERS=0).
An explicitly enabled pool without a processor fails startup. Go composition
through runWithProcessor supplies the future processor seam and exercises the
actual engine in integration tests without inventing an HTTP sender.

## Model and transitions

Migration0003 adds lease_token UUID, lease_expires_at timestamptz,
processing_attempts bigint default0, processed_at timestamptz. It replaces only
the status CHECK and adds coupling constraints and claim indexes; no rows deleted,
no earlier migration changed. Pending is still the insertion default.

| From | Condition | To | Persistent effect |
| --- | --- | --- | --- |
| pending | Known destination, fingerprint2, selected under row lock | processing | New random token, database-time expiry, attempts+1 |
| processing | Expiry <= database wall clock; eligible row locked | processing | New token and expiry, attempts+1; old token invalid |
| processing | Matching ID/token, lease not expired | processed | Clear lease, set processed_at using DB clock |
| processing | Error/cancel/crash | processing | No false success or early release; recovered after expiry |
| processed | Any claim attempt | processed | Terminal for this increment; never reclaimed |

pending has no lease or processed_at; processing has token+expiry, no processed_at;
processed has processed_at and no lease. Attempts never reset and must be >=0.
No handler or transaction holds row locks for the duration of processing.

## Transactional claim and ownership

Claim one event per idle worker using a short Read Committed transaction with
SELECT FOR UPDATE SKIP LOCKED in a CTE plus UPDATE RETURNING, then commit before
calling the processor. Select pending or expired processing rows; filter
`destination_id IS NOT NULL AND fingerprint_version=2`. Order by created_at,id
for a deterministic preference, without claiming strict FIFO/fairness. SKIP LOCKED
avoids waiting on another worker's row; it deliberately provides a queue view,
not a consistent reporting snapshot.

Lease times use clock_timestamp(), not application time or transaction-start
now(). The database is the authority. Complete is conditional on ID, current
token, processing state and unexpired lease; zero affected rows means lease lost.
Complete first locks the row by ID, then checks ownership/expiry in a separate
UPDATE within the same transaction, so expiry cannot be approved before an
unchanged locked tuple finishes waiting.
Tokens are fresh random UUIDs on every claim/reclaim and are never logged or
exposed via the HTTP event representation. PostgreSQL serializes claim/update;
we do not use process-local maps or channel exclusivity as persistence guarantees.

Invariants: at most one current unexpired lease per event; stale or expired
holders cannot confirm; callback starts only after committed claim; failed commit
cannot produce a callback; no destination-less or legacy-v1 row is processed;
processed rows are not claimed again. After restart all recovery information is
in PostgreSQL; no in-memory ownership is trusted.

## Pool lifecycle and configuration

WorkerConfig bounds worker count (1..32 for an active pool), lease duration
(100ms..24h), poll interval (>0..1min) and DB operation timeout (>0..lease duration).
Each worker owns at most one active callback, and polls only when idle; no
unbounded queue of already-leased work. Each SQL operation has a timeout. Empty
queue, DB errors and callback errors all lead to bounded polling, avoiding spins.
A callback gets a child context canceled on shutdown and bounded by the lease
interval; authoritative expiry is still checked in SQL. Callbacks must cooperate
with context cancellation; Go cannot kill a callback that ignores context.

Pool Run joins workers on parent cancellation. Application shutdown cancels the
pool and waits before closing its DB. Claims not confirmed on cancellation stay
leased for recovery rather than bypassing fencing with a background completion.
Errors are observable via an error callback/log with no payload/token/credentials.
No heartbeat or backoff schedule beyond waiting for lease expiry in this increment.

Runtime variables: GORELAY_WORKERS (0 default, max32), GORELAY_LEASE_DURATION
(30s default), GORELAY_POLL_INTERVAL (250ms default), GORELAY_WORKER_DB_TIMEOUT
(5s default). Invalid values fail before worker startup. Local bind, Host/Origin
restrictions and required-PostgreSQL CI from Stage2 remain in force.

## Legacy policy and validation plan

Legacy events with unknown destination remain unchanged and unclaimed. No guessed
destination, automatic backfill or attempts increment. They require separate human
review before any future eligibility change. Migration0002's NOT VALID constraint
must remain compatible with these untouched rows.

Validate with real PostgreSQL: two claimers/one event, locked-row skipping,
expired lease reclaim, stale token and expired-only completion rejected, completed
row terminal, two competing completions, pool bounds/cancellation/errors, process
kill after claim and fresh-process recovery, legacy preservation and Stage2 HTTP,
idempotency, migrations and local-security regressions. Required SQL tests must
not skip. Independent auditor must not edit implementation; record hashes and
commands. Calibration by mutations, real host power loss and network partitions
are not claimed without execution.

References: [PostgreSQL locking and SKIP LOCKED](https://www.postgresql.org/docs/17/sql-select.html)
and [database wall-clock semantics](https://www.postgresql.org/docs/17/functions-datetime.html).
