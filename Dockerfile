FROM golang:1.27.3-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd/ ./cmd/
RUN CGO_ENABLED=0 GOTOOLCHAIN=local go build -trimpath -o /gorelay ./cmd/gorelay

FROM scratch
COPY --from=build /gorelay /gorelay
COPY LICENSE LICENSING.md LICENSE-SCOPE.json ASSET-RIGHTS.md THIRD-PARTY-NOTICES.md /licenses/
COPY licenses/third-party/ /licenses/third-party/
# GoRelay only binds a literal loopback address, so inside the container it is
# reachable only from the container's own network namespace. Run it with
# `--network host` (as scripts/demo-local.py does); `-p 8080:8080` will NOT work.
ENV GORELAY_ADDR=127.0.0.1:8080
USER 65532:65532
ENTRYPOINT ["/gorelay"]
