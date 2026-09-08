# Astriena

**The OpenTelemetry BYOS observability engine — open-source core.**

Astriena is an OpenTelemetry-native observability platform built as a
**Bring Your Own Storage (BYOS)** alternative to incumbent SaaS vendors. This
repository holds the open-source core — the `astriena` binary, a high-throughput
dynamic tail-sampling proxy that filters redundant telemetry before it leaves your
network and writes compressed OTel data directly into your own ClickHouse cluster.

## Why Astriena

- **Zero telemetry markup (BYOS)** — data lands in your own storage (~$0.02/GB) instead
  of incumbent write costs of $0.10–$0.65/GB.
- **~80% ingestion reduction** — stateless tail sampling drops redundant `200 OK`
  traces before outbound transmission, retaining 100% of errors, latency spikes, and
  high-cardinality metadata.
- **No cardinality traps or lock-in** — pure OTLP ingress and standard ClickHouse SQL.

## Status

Early stage. The proxy code is being brought up in this repository. Watch or star to
follow along.

## License

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution
requirements.

"Astriena" is a trademark of the project's maintainers; the Apache 2.0 license does not
grant rights to use the name or marks.
