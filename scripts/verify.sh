#!/bin/sh
set -eu
# CI and local verification use this same fail-closed integration gate.
: "${TEST_DATABASE_URL:?TEST_DATABASE_URL must point to a test PostgreSQL database}"
export REQUIRE_POSTGRES_TESTS=1
printf '%s\n' 'Checking formatting'
test -z "$(gofmt -l cmd)"
go vet ./...
printf '%s\n' 'Unit and required PostgreSQL integration tests'
go test -count=1 ./...
printf '%s\n' 'Race detector and required PostgreSQL integration tests'
go test -race -count=1 ./...
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT
CGO_ENABLED=0 go build -buildvcs=false -o "$out/gorelay" ./cmd/gorelay
