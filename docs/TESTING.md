# Reproduce required tests with isolated Docker resources

Prerequisites: Docker Engine, an unused resource prefix and access to the Go and
PostgreSQL images/module proxy. Run from the repository root on Linux. These
commands use disposable synthetic credentials. They require no `.env` and never
connect to the ordinary Compose database. First confirm that none of the named
resources already exists; **stop if any exists**, choose another prefix and
substitute it consistently. Do not delete resources to make the names available.

```sh
docker network inspect gorelay-review-net
docker volume inspect gorelay-review-pgdata
docker container inspect gorelay-review-pg
docker container inspect gorelay-review-tests
# Each must report "not found" before continuing.
docker network create gorelay-review-net
docker volume create gorelay-review-pgdata
docker run -d --name gorelay-review-pg --network gorelay-review-net \
  --cpus=1 --memory=512m --memory-swap=512m --pids-limit=128 \
  -e POSTGRES_USER=review -e POSTGRES_PASSWORD=review-test-only \
  -e POSTGRES_DB=review -v gorelay-review-pgdata:/var/lib/postgresql/data \
  postgres:17 -c max_connections=60
# Wait until this exits 0; do not run tests before it is ready:
docker exec gorelay-review-pg pg_isready -U review -d review
# Pre-build a tooling image (fetches modules) before entering the dedicated test network:
docker build --target build -t gorelay:review-tooling .
docker run --name gorelay-review-tests --network gorelay-review-net \
  --cpus=2 --memory=1536m --memory-swap=1536m --pids-limit=256 \
  -v "$PWD:/src:ro" -w /src -e GOTOOLCHAIN=local -e GOMAXPROCS=2 \
  -e TEST_DATABASE_URL='postgres://review:review-test-only@gorelay-review-pg:5432/review?sslmode=disable' \
  gorelay:review-tooling sh scripts/verify.sh
docker build -t gorelay:review .
docker stop gorelay-review-pg
```

The build stage includes downloaded dependencies; source is mounted read-only.
Normal/race builds may need additional Go tooling/cache access; failures must be
reported rather than reclassified as passing. The test schemas are created and
dropped by tests; the database container and named volume remain preserved after
stopping. Do not use `down -v`, `prune`, or another project's database. To repeat,
use fresh names or deliberately review/reuse only these owned test resources.

`verify.sh` fails if SQL configuration is missing/unavailable. Ordinary `go test`
without required-SQL mode can skip integration tests; that is not the full gate.
Normal/race test output contains opt-in benchmark/experiment gates; they do not
run full performance experiments unless separately requested. Stage7A records
new runs separately from historical Stage6A/6B results.

The dedicated bridge is isolated by Docker project/resource ownership and has no
public ports. Outbound registry/module-proxy access may be needed for tooling and
test dependencies; it is not claimed to be an air-gapped network.
