# syntax=docker/dockerfile:1
FROM golang:1.27.3-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
# DuckDB is linked into the Go binary using CGO; no database server is needed.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go test ./... && CGO_ENABLED=1 go build -trimpath -ldflags='-s -w' -o /out/entreprises .

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /data && chown 65532:65532 /data
COPY --from=build /out/entreprises /usr/local/bin/entreprises
USER 65532:65532
ENV DATA_DIR=/data LISTEN_ADDR=:8080
EXPOSE 8080
VOLUME /data
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["/usr/local/bin/entreprises", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/entreprises"]
