FROM golang:1.27.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd/ ./cmd/
RUN CGO_ENABLED=0 GOTOOLCHAIN=local go build -trimpath -o /gorelay ./cmd/gorelay

FROM scratch
COPY --from=build /gorelay /gorelay
COPY LICENSE LICENSING.md LICENSE-SCOPE.json ASSET-RIGHTS.md THIRD-PARTY-NOTICES.md /licenses/
COPY licenses/third-party/ /licenses/third-party/
ENV GORELAY_ADDR=127.0.0.1:8080
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/gorelay"]
