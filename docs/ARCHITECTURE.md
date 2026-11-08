# Architecture

GoRelay is one Go executable (`cmd/gorelay`) with embedded dashboard assets and
SQL migrations. Keeping transport, worker and store code in one package favors
a small demonstrator over a reusable SDK; no external broker is involved.

`main.go` validates local configuration, opens a bounded SQL pool, migrates, then
runs the HTTP server and optional workers under cancellation. `events.go` owns
request validation and idempotent persistence. `leases.go` claims eligible rows
with SKIP LOCKED. `workers.go` runs one active processor per worker.
`http_processor.go` validates immutable local destinations and bounds requests.
`retry_store.go` atomically records fenced outcomes/history and schedules retries
or dead-letter state; `retry_policy.go` computes bounded jitter/delays.
`dashboard_store.go` supplies bounded read models; `dashboard_demo.go` owns
explicitly enabled synthetic controls and its local recipient.

An idempotency key and event share the same transactional row. Keys are global
and retained indefinitely. Stable event identity survives retries. A lease token
and database-time expiry fence SQL outcomes; they cannot undo external HTTP
side effects. Claims and attempt slots persist before callbacks. There is no
heartbeat, strict FIFO guarantee, early lease release or DLQ re-drive.

Migrations 0001–0005 are forward-only and embedded; checksum/version records and
DDL commit under one advisory transaction lock. Legacy destination-less/v1 rows
remain excluded from automatic delivery and require a reviewed backfill.

See the six [ADRs](adr/) for historical decisions and scope boundaries. Earlier
stage descriptions are historical, not claims that later features are absent.
