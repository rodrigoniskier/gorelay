# Local API and configuration

The Go executable does not read `.env`; Compose does. Export `DATABASE_URL`
separately for a native server. Never paste real credentials into public logs.
Use a dedicated PostgreSQL database; startup applies embedded migrations 0001–0005.

| Variable | Default / constraint |
| --- | --- |
| DATABASE_URL | Required pgx DSN; operator-controlled local database |
| GORELAY_ADDR | 127.0.0.1:8080; literal loopback and numeric port only |
| GORELAY_WORKERS | 0 disabled; 1–32 when enabled |
| GORELAY_LEASE_DURATION | 30s; 100ms–24h |
| GORELAY_POLL_INTERVAL | 250ms; positive, at most 1m |
| GORELAY_WORKER_DB_TIMEOUT | 5s; positive and no greater than lease |
| GORELAY_HTTP_DEMO | Explicit 1 enables configured local HTTP processor |
| GORELAY_DESTINATIONS | JSON map, 1–16 IDs to literal-loopback HTTP URLs |
| GORELAY_HTTP_TIMEOUT | 5s; positive, at most 1m |
| GORELAY_HTTP_CONNECT_TIMEOUT | 1s; positive, at most request timeout |
| GORELAY_MAX_ATTEMPTS | 5; 1–100; pinned on first claim |
| GORELAY_RETRY_BASE_DELAY | 1s; 1ms–1h |
| GORELAY_RETRY_MAX_DELAY | 1m; at least base, at most 24h |
| GORELAY_DASHBOARD_DEMO | Explicit 1 plus workers enables synthetic lab controls |

Require HTTP timeout + 2 × worker DB timeout < lease duration. No heartbeat
extends the lease. The built-in dashboard recipient replaces manually configured
HTTP destinations while that demonstration is enabled. Retry settings and
validation are defined in `retry_policy.go`; the demo helper uses 1s base / 4s cap
and the default five attempt slots. These are demonstration settings.

After starting the app and a configured recipient, for example ID `demo`:

```sh
curl --fail-with-body http://127.0.0.1:8080/readyz
curl -i http://127.0.0.1:8080/v1/events \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: example-order-1' \
  -d '{"destination_id":"demo","type":"order.created","payload":{"synthetic":true}}'
# GET the Location returned by POST using the same local base URL.
```

POST returns 201 for creation, 200 for matching replay, 409 for conflicting key
reuse. Type is nonblank and at most 128 bytes; destination ID is 1–128 ASCII
characters matching `[A-Za-z0-9][A-Za-z0-9._:-]*`. Keys are 1–200 visible ASCII
bytes. Payload must be a JSON object; unknown fields/trailing JSON fail. Body
limit is 1 MiB. Object order/whitespace do not affect fingerprints; numeric
lexemes do (1 differs from 1.0). Valid but unconfigured destinations can be stored;
workers fail them without making an arbitrary network request.

GET `/v1/events/{uuid}` returns 400 for invalid UUID, 404 for missing event.
Lease tokens are not exposed. `/healthz` checks liveness; `/readyz` checks schema
and DB. DB operations have five-second deadlines; readiness two seconds and
startup 30 seconds. Graceful shutdown has a ten-second HTTP drain budget. On
SIGINT/SIGTERM workers stop claiming, and an in-flight delivery gets up to eight
seconds to finish; its outcome is still recorded (delivered or failure) instead of
being abandoned to lease expiry, so a restart does not consume an extra attempt or
resend an event the recipient already accepted. A callback that outlives that grace
is canceled. The database pool is sized `max(20, workers + 8)` connections. Idle
workers poll with a read-only probe; the locking exhaustion sweep writes only when
exhausted events exist. Migrations must be a contiguous applied prefix; a gap fails
startup and readiness.
There is no authentication or public networking support.
