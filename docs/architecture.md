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
├── go.mod                        # root module: the pure core (no OTel deps)
├── builder-config.yaml           # ocb manifest for the distribution
├── config.yaml                   # example runtime config
├── Makefile                      # test/lint the pure core; build the distro
├── internal/
│   ├── sampling/                 # PURE tail-sampling engine (+ tests)
│   └── clickhouse/               # PURE ClickHouse writer
├── components/
│   ├── astrienasampler/          # thin Collector processor adapter (own go.mod)
│   └── clickhouseexporter/       # thin Collector exporter adapter (own go.mod)
└── .github/workflows/ci.yml      # CI: pure core + full distro build
```

### Modules

This is a multi-module repo, which the Collector Builder requires: every local
component it compiles in must be its own Go module.

- **Root module** `github.com/rockbox37/astriena` — the pure core under
  `internal/`. It has **no OpenTelemetry dependency**, so it builds and tests
  offline and fast (this is what keeps the core portable and benchmarkable).
- **Component modules** under `components/*` — each has its own `go.mod`, depends
  on the OTel Collector, and reaches the core via `replace … => ../../`.

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

## Releasing (for the private cloud repo to consume)

`astriena` is the upstream source of truth. Cut **semver git tags**; publish the
binary and a container image. The private `astriena-cloud` control plane consumes
**pinned released versions** (image/package), never proxy source. See the private
repo's `docs/repo-strategy.md`.
