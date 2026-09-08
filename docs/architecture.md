# Astriena Architecture

## Positioning

> **A focused OpenTelemetry distribution with a sampling engine that uses a
> fraction of the memory of the stock processor — none of the plugin bloat.**

Astriena speaks OTLP in, applies dynamic tail sampling, and writes the kept
telemetry into the customer's **own** ClickHouse cluster (Bring Your Own Storage).
Data never touches Astriena's infrastructure.

## Shape: a custom Collector distribution with a pure core

Astriena is built as a **custom OpenTelemetry Collector distribution** assembled
by the OpenTelemetry Collector Builder (`ocb`, see [`builder-config.yaml`](../builder-config.yaml)).
It reuses the Collector's hardened OTLP receiver and batch/queue plumbing, and
adds two custom components:

- **`astriena_sampler`** — the tail-sampling processor
  ([`components/astrienasampler`](../components/astrienasampler))
- **`clickhouse`** — the BYOS exporter
  ([`components/clickhouseexporter`](../components/clickhouseexporter))

### Why a distro (and why it doesn't blunt the wedge)

The Collector's reputation for being heavy is about the giant *contrib* binary's
plugin sprawl and the stock `tail_sampling` processor's memory behavior — **not**
the framework core. A custom distro compiles in only what we ship, so it stays
small. The memory wedge lives in the **sampling engine**, which we write
ourselves — so building on the Collector buys us production-grade OTLP/backpressure
plumbing for free without giving up the differentiator. (Full trade-off analysis:
the private strategy doc.)

## The hexagonal rule: keep the core pure

The two pieces of real IP are plain Go packages with **no dependency on the
Collector or OTLP types**:

- [`internal/sampling`](../internal/sampling) — the tail-sampling engine
- [`internal/clickhouse`](../internal/clickhouse) — the ClickHouse writer

The Collector components are **thin adapters**: they translate `pdata` <-> the
engine's domain types and own no business logic.

```
  OTLP in ──▶ [otlpreceiver] ──▶ [astriena_sampler adapter] ──▶ [batch] ──▶ [clickhouse adapter] ──▶ ClickHouse (BYOS)
                                        │                                          │
                                        ▼                                          ▼
                              internal/sampling (pure)                  internal/clickhouse (pure)
```

Keeping the core pure is deliberate: it lets us swap the surrounding framework,
extract a standalone binary, or reimplement the hot path in another language
without rewriting the sampling logic — and it lets us benchmark the engine in
isolation.

## Deferred: a possible Rust hot path

Astriena is Go for time-to-market, ecosystem reuse, and contributor reach (see
the private strategy doc's language decision). The performance-critical
sampling engine is a **candidate for a later Rust reimplementation** once
benchmarks prove Go's GC/memory is the real bottleneck at a paying customer's
scale — a data-driven trigger, not a speculative one.

- **Most likely path:** a standalone Rust rewrite of the engine as its own
  binary. Because `internal/sampling` is a clean package behind an interface, its
  design ports over — we re-express proven logic rather than redesign.
- **FFI caveat:** if instead calling Rust from Go via cgo, cross the boundary
  **per batch, never per span** — per-span cgo overhead would erase the gains.
- **Not worth it until measured.** Two toolchains and a smaller contributor pool
  for that component are real costs; adopt Rust only against evidence.

## Repository layout

```
astriena/
├── builder-config.yaml           # ocb manifest for the distribution
├── config.yaml                   # example runtime config
├── Makefile                      # test/lint the pure core; build the distro
├── internal/
│   ├── sampling/                 # PURE tail-sampling engine (+ tests)
│   └── clickhouse/               # PURE ClickHouse writer
├── components/
│   ├── astrienasampler/          # thin Collector processor adapter
│   └── clickhouseexporter/       # thin Collector exporter adapter
└── .github/workflows/ci.yml      # CI: build/vet/test the pure core
```

## Build

```
make test     # pure core, offline
make build    # assemble the astriena distribution via ocb into ./_build
```

The pure core builds and tests with plain `go` offline. The full distribution
requires the Collector module versions in `builder-config.yaml` to be pinned to a
verified release first (they ship as a starting point and must be bumped/checked).

## Releasing (for the private cloud repo to consume)

`astriena` is the upstream source of truth. Cut **semver git tags**; publish the
binary and a container image. The private `astriena-cloud` control plane consumes
**pinned released versions** (image/package), never proxy source. See the private
repo's `docs/repo-strategy.md`.
