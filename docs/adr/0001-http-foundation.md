# 0001. Start with a single HTTP service

**Status** Accepted for stage 1

## Context

GoRelay needs a runnable baseline that lets developers verify the HTTP process lifecycle before introducing event persistence and delivery.

## Decision

Use Go's standard library and one executable at `cmd/gorelay`. Serve `GET /healthz` as a process liveness check. Bound header size, request reads, response writes, and idle connections. Drain active HTTP requests for up to 10 seconds on SIGINT or SIGTERM. Default to loopback on the host and allow the container to listen on its own network interface.

Use the same Go release for local builds, CI, and the Docker build stage. Ship only the static executable in the runtime image and run it with a numeric non-root UID and GID.

## Consequences

The baseline has no external Go modules or runtime services. A successful health check confirms that the HTTP process responds. Readiness, durable event storage, and delivery are future stages. The minimal image has no shell, so diagnostics use the HTTP endpoint and process logs.
