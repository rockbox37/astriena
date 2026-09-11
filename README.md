<p align="center">
  <img src="docs/assets/astriena-icon-512.png" width="128" height="128" alt="Astriena constellation mark">
</p>
<h1 align="center">Astriena</h1>
<p align="center"><em>of the stars</em></p>

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
make build    # assemble the astriena distribution into ./_build/astriena

make clickhouse-up        # local ClickHouse on :9000 (skip if you already have a cluster)
make clickhouse-bootstrap # optional: pre-create the astriena database in Docker

ASTRIENA_CLICKHOUSE_DSN=clickhouse://localhost:9000/astriena \
  ./_build/astriena --config config.yaml
```

The exporter creates the target database on connect if it does not exist (`CREATE DATABASE IF NOT EXISTS`).
`make clickhouse-bootstrap` remains a convenience for local Docker setups.
See [docs/clickhouse-integration.md](docs/clickhouse-integration.md) for driver notes.

Self-metrics (sampler drops, ClickHouse flush counters) are exposed on `:8888/metrics`
via the `service.telemetry` Prometheus reader in `config.yaml` — not mixed into the
ClickHouse trace pipeline. See [docs/observability.md](docs/observability.md) for the
metric catalog and suggested alerts.

> `make build` fetches the OpenTelemetry Collector modules pinned in
> [`builder-config.yaml`](builder-config.yaml) (the `v0.160.0` / `v1.66.0` line)
> and produces a runnable binary. The pure core (`make test`) builds offline with
> no OTel dependency.

### Run from GHCR

Release images are published to `ghcr.io/rockbox37/astriena` (tag matches the
GitHub release, e.g. `v0.1.0`). OTLP listens on `:4317` (gRPC) and `:4318` (HTTP);
self-metrics on `:8888/metrics` — see [docs/observability.md](docs/observability.md).

```sh
make clickhouse-up   # or use your own cluster

docker run -d --name astriena \
  --platform linux/amd64 \   # required on Apple Silicon (image is amd64-only for now)
  -p 4317:4317 -p 4318:4318 -p 8888:8888 \
  -e ASTRIENA_CLICKHOUSE_DSN=clickhouse://host.docker.internal:9000/astriena \
  ghcr.io/rockbox37/astriena:v0.1.0
```

On Docker Desktop, `host.docker.internal` reaches ClickHouse on the host (e.g.
after `make clickhouse-up`). The exporter creates the database on connect;
`make clickhouse-bootstrap` is optional for local Docker.

Minimal smoke after the container starts:

```sh
curl -s localhost:8888/metrics | head
curl -s -X POST localhost:4318/v1/traces \
  -H 'Content-Type: application/json' -d '{"resourceSpans":[]}'
# tail sampling waits decision_wait (5s in config.yaml) before counters move
```

## Status

Early stage — scaffolding in place, core engine under construction. The pure
sampling engine ([`internal/sampling`](internal/sampling)) and ClickHouse writer
([`internal/clickhouse`](internal/clickhouse)) are the growth areas; look for
`TODO(core)` markers. Watch or star to follow along.

## Visual language

The mark pairs an irregular mixed-magnitude night sky with a pale-gold capital
**A** in [Cinzel Decorative](https://github.com/google/fonts/tree/main/ofl/cinzeldecorative),
designed by Natanael Gama and licensed under the SIL Open Font License 1.1.
Social lockups use the full **Astriena** wordmark in the same face. Letterforms
are committed as outlined SVG paths so GitHub rendering does not depend on an
installed font.
Night-sky tokens used across the SVG assets in [`docs/assets/`](docs/assets/):

| Token | Hex | Role |
|---|---|---|
| `bg` | `#0B1220` | Night sky |
| `surface` | `#141C2E` | Raised panel / gradient peak `#1A2740` |
| `star` | `#E8D5A3` | Star body and accent |
| `star-core` | `#F7F1E1` | Bright star core |
| `dim` | `#5C6B84` | Distant field stars |
| `text` | `#E8EEF7` | Primary text on sky |
| `text-muted` | `#9AA8BC` | Secondary text |

Source of truth is the SVG. `astriena-mark.svg` is the app/avatar mark;
`astriena-mark-16.svg` / `favicon.svg` are the 16px silhouette; `astriena-social.svg`
is the typeset 1280×640 preview. Matching PNGs (`astriena-icon-512.png`,
`astriena-icon-64.png`, `astriena-icon-16.png`, `astriena-social.png`) are for
GitHub, which wants raster uploads. Org/repo avatar and Settings → Social
preview still need a maintainer upload.

To regenerate every SVG and PNG from the unmodified vendored TTF, create a
temporary virtual environment, install the pinned `fonttools` and `cairosvg`
versions, and run
[`docs/assets/render_png.py`](docs/assets/render_png.py). The exact commands are
documented at the top of that script; no Python dependencies are added to the Go
module.

## License

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for
attribution requirements. "Astriena" is a trademark of the project's maintainers;
the Apache 2.0 license does not grant rights to use the name or marks.
