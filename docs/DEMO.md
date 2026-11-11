# GoRelay local failure lab

This demonstration requires Docker, Docker Compose,
Python3 and a local `.env` based on `.env.example` with your own PostgreSQL
password. Credentials stay in the backend; Docker administrators can inspect
container environment variables. Never publish these containers or the API.

Start PostgreSQL and build the image locally:

```sh
docker compose up -d postgres
docker compose ps
docker build -t gorelay:dashboard-demo .
python3 scripts/demo-local.py
```

Wait for `GoRelay is listening` before opening
**http://127.0.0.1:18084/dashboard/**. The script uses a dedicated persistent
`gorelay_dashboard_demo` schema, not the default API schema. Repeated runs retain
its events. It never removes containers or volumes. If its named container already
exists, use `docker start -a gorelay-dashboard-demo` rather than creating another.

### WSL / Docker Desktop

Host networking can refer to the Docker VM rather than the browser host. This
was observed during validation: the container listener was healthy but WSL
Chromium received connection refused. `scripts/demo-local.py` now detects Docker
Desktop and does the following automatically: it copies the static binary out of the
image with a throw-away container (created and removed by the script) and runs it
natively, so the plain `python3 scripts/demo-local.py` is enough. Pass `--docker`
to force the container variant anyway. To do the same by hand with a binary you
keep, use the image binary locally:

```sh
docker create --name gorelay-dashboard-artifact gorelay:dashboard-demo
docker cp gorelay-dashboard-artifact:/gorelay /tmp/gorelay-dashboard
python3 scripts/demo-local.py --native /tmp/gorelay-dashboard
```

The artifact container stays preserved. Reuse it for later `docker cp`; do not
repeat `docker create` with the same name. Do not run both variants on port18084.
Stop the native process with Ctrl+C (normal Go graceful shutdown), then
`docker compose stop postgres`. This native variant was used for browser
validation at the same effective URL above.

The read-only dashboard is also available on the ordinary local application at
`http://127.0.0.1:8080/dashboard/`. By default `GORELAY_WORKERS=0` and the demo
controls are disabled. `GORELAY_DASHBOARD_DEMO=1` plus explicit positive workers
activates the isolated recipient and controls; existing `GORELAY_HTTP_DEMO`
configuration remains supported when the dashboard demo is disabled. The script
uses four workers, five default attempts, a1s base and4s cap; these are demo
settings, not performance recommendations. Recipient URL is server-generated on
an ephemeral loopback port, never entered by the user.

## Reproducible walkthrough

1. Select **200 Healthy**, generate10 synthetic events and observe Delivered.
2. Select **503 Unavailable**, wait at least5 seconds between batches, and generate
   another10. Observe Scheduled retry and Inspect an event's recorded attempts.
3. Restore **200 Healthy** promptly. Still-eligible events can recover. Requests
   already in flight may finish under the previous mode.
4. Set **503** again, generate a batch, and leave it until its five consumed slots
   exhaust. Show the red Dead-letter count and attempt history.
5. Restore **200**. Dead-letter events stay retained; no automatic or manual
   re-drive exists in this stage. Generate a fresh batch after the cooldown to
   show new successful delivery.
6. Optional: **429** returns Retry-After1s; **Timeout** waits for client cancellation
   (default request timeout5s), producing recorded transient failures.

Each batch is1..100 events and atomic. Cooldown is5 seconds per application
process, including generation failures. Synthetic events use logical destination
`dashboard-demo`, type `demo.delivery`, and payload `{"synthetic":true}`. Metrics
come from PostgreSQL and include any previous demo runs, never fabricated data.

Success denominator is delivered + dead_letter; pending/retry/processing and
legacy generic processed events are excluded. Empty denominators and unavailable
latency display a dash. Mean successful attempt measures recorded claim-to-finish
SQL timestamps, not network-only latency. Worker activity means active leases,
not a process-heartbeat inventory. Latest100 events/attempts are visible. Use
Inspect again to refresh a detail snapshot; overview polling is every3s.

Finite retries do not guarantee successful receipt. A crash can consume a slot
before sending. Accepted HTTP followed by failed SQL confirmation can duplicate;
consumers must deduplicate the stable GoRelay-Delivery-ID. Future DLQ re-drive
requires an explicit reviewed policy and is absent here.

## Stop without deleting state

In another terminal:

```sh
docker stop gorelay-dashboard-demo
docker compose stop postgres
```

Restart using `docker compose start postgres`, wait for health, then
`docker start -a gorelay-dashboard-demo`. Stopping does not delete data. Do not
run `down -v`, `rm`, `clean` or schema deletion. If PostgreSQL connection settings
change, review the preserved container configuration before restart.

## First run from a fresh checkout

Before the startup commands above, run `cp .env.example .env`, replace the local
password, choose a free PostgreSQL port if needed, then run
`docker compose config --quiet`. Use `docker compose up -d --wait postgres` so the
database is healthy before the helper. Python3 is required; host Go is optional
when extracting the Docker-built binary. No audit evidence or ignored file other
than your newly created `.env` is needed. Docker image/module downloads require
network access. The supported desktop fallback is the native binary described above.

For native mode, the named container stop command does not stop the native Go
process: Ctrl+C in its terminal does. Stop only the Compose project you started.
The existing-container restart instructions assume the same image/configuration;
review those settings if you have rebuilt or changed the database. Preserve data.

## Presentation script (approximately 4 minutes)

Preflight off camera: complete first-run setup/build, confirm `/readyz`200,
choose a clean owned demo schema or explain retained counters, open the dashboard
and close terminals that might show credentials. Never drop a schema to reset a demo.

| Time | Demonstration and explanation |
| --- | --- |
| 0:00–0:40 | Introduce durable event state, Go/PostgreSQL and the local-only boundary; show the architecture diagram |
| 0:40–1:20 | Select200, generate10 events, inspect stable identity and successful attempt history |
| 1:20–2:10 | Select503, generate10 after cooldown, show scheduled retries; restore200 before the finite budget exhausts |
| 2:10–3:10 | Select503 again, generate another batch after cooldown, explain bounded attempts and wait for DLQ; if scheduling takes longer, explain rather than fabricate completion |
| 3:10–3:40 | Restore200; show DLQ remains retained without re-drive; discuss duplicates and database fencing |
| 3:40–4:20 | Show reproducible test/evidence links, adverse worker-scaling result and30-second pilot; stop only owned processes |

This is a proposed recording script, not a video or evidence of full keyboard,
screen-reader, production or sustained-load certification. No recording is made
in Stage7A. Use neutral synthetic data and inspect screenshot metadata before publication.
