# Multi-stage build for the Astriena OpenTelemetry Collector distribution.
#
# Build:  docker build -t astriena .
# Run:    docker run --rm -p 4317:4317 -p 4318:4318 -p 8888:8888 \
#           -e ASTRIENA_CLICKHOUSE_DSN=clickhouse://host.docker.internal:9000/astriena \
#           ghcr.io/rockbox37/astriena:v0.1.0

ARG GO_VERSION=1.26
ARG OCB_VERSION=0.160.0

FROM golang:${GO_VERSION}-bookworm AS builder

ARG OCB_VERSION
WORKDIR /src

COPY . .

RUN go install go.opentelemetry.io/collector/cmd/builder@v${OCB_VERSION}
RUN builder --config builder-config.yaml

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=builder /src/_build/astriena /astriena
COPY config.yaml /config.yaml

EXPOSE 4317 4318 8888

ENTRYPOINT ["/astriena"]
CMD ["--config", "/config.yaml"]
