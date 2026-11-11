# GoRelay

A local event-delivery lab built with Go, PostgreSQL and Docker. GoRelay accepts
HTTP events, persists them transactionally and delivers them through bounded
workers with retries, expiring leases and fenced database acknowledgements.
It is a portfolio project for studying recovery and concurrency, not a
production delivery service.

![GoRelay local dashboard](docs/assets/dashboard.png)

## Engineering features

- Transactional event persistence and globally unique idempotency keys: matching
  replays reuse the event; conflicting reuse returns HTTP 409.
- PostgreSQL `FOR UPDATE SKIP LOCKED`, bounded workers, expiring ownership leases
  and token fencing against stale SQL outcomes.
- Explicit loopback HTTP recipients, bounded exponential retries with jitter,
  recorded attempt history and retained dead-letter events; no re-drive.
- Embedded dashboard with synthetic 200/429/503/timeout scenarios.
- Real PostgreSQL integration, race detection and controlled recovery tests.

```mermaid
flowchart LR
  Client -->|POST + Idempotency-Key| API[Loopback Go API]
  API -->|transaction| PG[(PostgreSQL)]
  PG -->|claim + lease token| Workers[Bounded workers]
  Workers -->|POST + stable delivery ID| Target[Loopback recipient]
  Workers -->|fenced outcome + history| PG
  PG --> Dashboard[Local dashboard]
```

A claim commits before HTTP starts. A crash before sending can consume an attempt.
A recipient accepting a POST before failed SQL confirmation can receive it again.
Consumers should durably deduplicate `GoRelay-Delivery-ID`. Fencing protects SQL
transitions, not external side effects. Finite attempts mean some events never
arrive. There is no exactly-once or universal delivery guarantee.

Known lab limits: events and attempt history are never pruned, and the dashboard
overview recomputes its counters with full-table aggregates every few seconds per
open tab, so it is meant for small demo datasets, not large backlogs.

## Run locally

Requirements: Docker Engine + Compose, Python 3 and a browser. Docker builds use
Go 1.27.4; PostgreSQL 17 is the database target. Host Go is optional for the Docker
route. Services must remain on loopback; do not expose or tunnel them.

```sh
git clone https://github.com/rodrigoniskier/gorelay.git
cd gorelay
cp .env.example .env
# Edit .env: replace POSTGRES_PASSWORD with a new local-only password.
# Choose an unused POSTGRES_PORT if 5432 is occupied.
docker compose config --quiet
docker compose up -d --wait postgres
docker build -t gorelay:dashboard-demo .
python3 scripts/demo-local.py
```

Open **http://127.0.0.1:18084/dashboard/** after the listening message. Choose
**200 Healthy**, generate 10 events and inspect attempt history. Switch to 503 to
observe retries, restore 200 while events remain eligible, or leave 503 until
attempts exhaust to demonstrate dead-letter retention. Wait five seconds between
batches. Counters persist across restarts; they need not begin at zero.

Docker Desktop/WSL host networking may refer to the Docker VM. The
[native-binary fallback and four-minute walkthrough](docs/DEMO.md) describe the
supported alternative. The application does not load `.env`; the helper reads
the owned Compose settings without printing credentials.

To stop the application container you started, use another terminal:

```sh
docker stop gorelay-dashboard-demo
docker compose stop postgres
```

In native mode, stop the Go process with Ctrl+C instead. Containers and volumes
remain preserved; no `down -v`, `prune` or database deletion is necessary.

## Test and build

With host Go 1.27.4 and a **dedicated test PostgreSQL 17 database**:

```sh
export TEST_DATABASE_URL='postgres://TEST_USER:TEST_PASSWORD@127.0.0.1:TEST_PORT/TEST_DB?sslmode=disable'
sh scripts/verify.sh
docker build -t gorelay:local .
```

Replace placeholders and never use another project's or a production database.
Tests create/drop uniquely named fixture schemas and require schema-creation
privileges. The verification script checks gofmt without rewriting, vet, normal
and race tests, then a static Go build. PostgreSQL is mandatory; missing SQL
configuration fails the gate. Experiment tests have separate opt-in gates and
are not evidence of benchmarks running during ordinary verification.

[Docker-only isolated test commands](docs/TESTING.md) ·
[Architecture](docs/ARCHITECTURE.md) · [Configuration/API](docs/CONFIGURATION.md) ·
[Troubleshooting](docs/TROUBLESHOOTING.md) · [Design decisions](docs/adr/)

## Measured evidence, with limits

[Benchmark methodology and results](docs/benchmarks/RESULTS.md) distinguish two
experiments. Stage 5A changes batch size and worker count together, so it does
not isolate scaling. Stage 5B fixes 1, 000 events per run, three repeats each at
1/4/8 workers: 9, 000 deliveries were SQL-reconciled. **Throughput decreased with
more workers** in that shared WSL/Docker environment; no definitive cause was
established. Go was limited to 2 CPUs /1536 MiB and PostgreSQL to 1 CPU /512 MiB.
The stability evidence is a 300-event /30-second pilot, not a completed two-minute
sustained test. These short batches are not production capacity or guaranteed
throughput. Curated CSVs, plots and checksums preserve the numeric observations.

## Security and boundaries

Workers default to zero; demo controls are opt-in. Only loopback listeners and
configured literal-loopback HTTP destinations are accepted. DNS targets,
redirect-following and environment proxies are excluded from delivery. There is
**no authentication**. Local processes and Docker administrators remain trusted;
a loopback recipient could itself forward traffic. Host/Origin checks do not
secure public reverse proxies. Never deploy this demonstration publicly.

[SECURITY.md](SECURITY.md) explains private vulnerability reporting.
[Dependency review](docs/DEPENDENCIES.md) separates prior module/package/symbol
matches from demonstrated exploitation and records the validated update scope.
No scanner result proves absolute absence of vulnerabilities. Full accessibility
is not certified, sustained production capacity is unproven, and production
security is not approved. Exact crash placement before the first send, complete
late-response capture, real partitions/physical failures and mutation calibration
remain incompletely demonstrated.

## Licensing

The standard [MIT license](LICENSE) applies **only to the 20 original application
source files explicitly listed in [LICENSE-SCOPE.json](LICENSE-SCOPE.json)**,
copyright (c) 2026 Rodrigo Niskier Ferreira Barbosa. This is not a repository-wide
MIT grant. [LICENSING.md](LICENSING.md) defines exclusions.

Historical visuals have independent reuse rights reserved under
[ASSET-RIGHTS.md](ASSET-RIGHTS.md), subject to third-party rights, platform terms
and law. Documentation, tests/scripts, configuration and evidence are not
MIT automatically. Dependencies retain upstream terms; see
[THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

This public repository is a deliberately selected representation with fresh,
privacy-preserving history. Large raw local audit logs and private historical
metadata are not included. [Validation and provenance](docs/VALIDATION.md)
records the exact tested scope and limitations. [Contributing](CONTRIBUTING.md).
