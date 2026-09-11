<p align="center">
  <img src="assets/astriena-icon-512.png" width="64" height="64" alt="Astriena constellation mark">
</p>

# Astriena Architecture

## Positioning

> **A focused OpenTelemetry distribution that cuts trace egress ~80% at the
> source, with a framework-free sampling engine — none of the plugin bloat.**

Astriena speaks OTLP in, applies dynamic tail sampling, and writes the kept
telemetry into the customer's **own** ClickHouse cluster (Bring Your Own Storage).
Data never touches Astriena's infrastructure.

The claim above is the measured one: **ingestion reduction**. Per-call ingest is
also cheaper in allocations than the stock processor, a figure that reproduces
run to run. Adapter-level in-flight heap is a **tie**, so memory is not sold as
an advantage. See [`benchmarks.md`](benchmarks.md) for the figures, the
engine-side bookkeeping cost, and the caveats on timings.

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
plugin sprawl and the stock `tail_sampling` processor's decision-loop design —
**not** the framework core. A custom distro compiles in only what we ship, so it
stays small. The differentiator lives in the **sampling engine** — what it drops
before egress, and what each ingested trace costs — which we write ourselves, so
building on the Collector buys us production-grade OTLP/backpressure plumbing for
free without giving up that differentiator. (Full trade-off analysis:
the private strategy doc.)

## The hexagonal rule: keep the core pure

The two pieces of real IP are plain Go packages with **no dependency on the
Collector or OTLP types**:

- [`internal/sampling`](../internal/sampling) — the tail-sampling engine
- [`internal/clickhouse`](../internal/clickhouse) — the ClickHouse writer

The Collector components are **thin adapters**: they translate `pdata` <-> the
engine's domain types and own no business logic.

The ClickHouse write path keeps this rule even though it needs a network driver:
`internal/clickhouse` holds the batching and auto-schema *logic* behind an
`Inserter` port and imports no driver, so it stays offline-buildable and
unit-testable with a fake. The real clickhouse-go/v2 binding (compressed
prepared-batch inserts, base-schema creation, and sparse per-attribute columns
with bloom-filter indexes) lives in the `clickhouseexporter` adapter and is
injected into the pure `Writer`. Keeping the driver in the adapter module is what
preserves the root module's zero-heavy-deps invariant below.

```
  OTLP in ──▶ [otlpreceiver] ──▶ [astriena_sampler adapter] ──▶ [batch] ──▶ [clickhouse adapter] ──▶ ClickHouse (BYOS)
                                        │                                          │
                                        ▼                                          ▼
                              internal/sampling (pure)                  internal/clickhouse (pure)
```

Keeping the core pure is deliberate: it lets us swap the surrounding framework,
extract a standalone binary, or reimplement the hot path in another language
without rewriting the sampling logic — and it lets us benchmark the engine in
isolation (see [`benchmarks.md`](benchmarks.md), which establishes the baseline
the reduction and ingest-cost claims rest on before any hot-path optimization).

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
  for that component are real costs; adopt Rust only against evidence. The
  adapter-level head-to-head in [`benchmarks.md`](benchmarks.md) does **not**
  show Go losing on in-flight heap vs stock; do not start a rewrite
  on the back of those numbers.

## Repository layout

```
astriena/
├── go.mod                        # root module: the pure core (no OTel deps)
├── builder-config.yaml           # ocb manifest for the distribution
├── config.yaml                   # example runtime config
├── Makefile                      # test/lint the pure core; build the distro
├── docs/
│   ├── architecture.md
│   ├── observability.md          # self-telemetry metrics, alerts, scrape config
│   └── assets/                   # constellation mark, favicon, social preview
├── internal/
│   ├── sampling/                 # PURE tail-sampling engine (+ tests)
│   └── clickhouse/               # PURE ClickHouse writer
├── components/
│   ├── astrienasampler/          # thin Collector processor adapter (own go.mod)
│   └── clickhouseexporter/       # thin Collector exporter adapter (own go.mod)
├── bench/                        # head-to-head vs stock tail_sampling (own go.mod)
└── .github/workflows/ci.yml      # CI pipeline (jobs defined in the workflow)
```

### Modules

This is a multi-module repo, which the Collector Builder requires: every local
component it compiles in must be its own Go module.

- **Root module** `github.com/rockbox37/astriena` — the pure core under
  `internal/`. It has **no OpenTelemetry dependency**, so it builds and tests
  offline and fast (this is what keeps the core portable and benchmarkable).
- **Component modules** under `components/*` — each has its own `go.mod`, depends
  on the OTel Collector, and reaches the core via `replace … => ../../`.
- **`bench/`** — head-to-head vs contrib `tailsamplingprocessor`. Own `go.mod`
  so the root module stays Collector-free; see [`benchmarks.md`](benchmarks.md).

Cross-module gotcha (documented in `builder-config.yaml`): a dependency's own
`replace` is ignored by the generated distribution module, so the manifest's
top-level `replaces:` re-declares the root-module replace for the `ocb` build.

## Build

```
make test     # pure core, offline
make build    # assemble the astriena distribution via ocb into ./_build/astriena
```

Collector module versions are **pinned** in `builder-config.yaml` to the
`v0.160.0` / `v1.66.0` release line; `make build` produces a runnable
`./_build/astriena`. Verify the components and an example config with:

```
./_build/astriena components
ASTRIENA_CLICKHOUSE_DSN=clickhouse://localhost:9000/astriena \
  ./_build/astriena validate --config config.yaml
```

For self-telemetry (Prometheus scrape endpoint, metric catalog, alert hints), see
[`observability.md`](observability.md).

For the active release plan, see [`roadmap-v0.2.md`](roadmap-v0.2.md).

## Releasing (for the private cloud repo to consume)

`astriena` is the upstream source of truth. Cut **semver git tags**; publish the
binary and a container image. The private `astriena-cloud` control plane consumes
**pinned released versions** (image/package), never proxy source. See the private
repo's `docs/repo-strategy.md`.
