# Astriena

> **A focused OpenTelemetry distribution with a sampling engine that uses a
> fraction of the memory of the stock processor — none of the plugin bloat.**

Astriena is an OpenTelemetry-native observability engine built as a **Bring Your
Own Storage (BYOS)** alternative to incumbent SaaS vendors. It speaks OTLP in,
applies dynamic tail sampling, and writes compressed telemetry directly into your
**own** ClickHouse cluster. Your data never touches Astriena's infrastructure.

## Why Astriena

- **Zero telemetry markup (BYOS)** — data lands in your own storage (~$0.02/GB)
  instead of incumbent write costs of $0.10–$0.65/GB.
- **~80% ingestion reduction** — stateless tail sampling drops redundant `200 OK`
  traces before outbound transmission, retaining 100% of errors, latency spikes,
  and high-cardinality metadata.
- **No cardinality traps or lock-in** — pure OTLP ingress and standard ClickHouse SQL.

## How it works

Astriena is a custom OpenTelemetry Collector distribution: it reuses the
Collector's hardened OTLP receiver and batching, and adds two components of its
own — the **`astriena_sampler`** tail-sampling processor and the **`clickhouse`**
BYOS exporter. The sampling engine and ClickHouse writer are framework-free Go
packages; the Collector components are thin adapters around them. See
[docs/architecture.md](docs/architecture.md).

Migrate in one line: point your existing Collector's OTLP exporter at Astriena.

## Quick start

```sh
make test     # build & test the pure core (offline)
make build    # assemble the astriena distribution into ./_build
./_build/astriena --config config.yaml
```

> Note: the full `make build` fetches the OpenTelemetry Collector modules pinned
> in [`builder-config.yaml`](builder-config.yaml). Those versions ship as a
> starting point — verify/bump them to the current Collector release first.

## Status

Early stage — scaffolding in place, core engine under construction. The pure
sampling engine ([`internal/sampling`](internal/sampling)) and ClickHouse writer
([`internal/clickhouse`](internal/clickhouse)) are the growth areas; look for
`TODO(core)` markers. Watch or star to follow along.

## License

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for
attribution requirements. "Astriena" is a trademark of the project's maintainers;
the Apache 2.0 license does not grant rights to use the name or marks.
