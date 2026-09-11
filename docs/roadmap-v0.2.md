# v0.2.0 Roadmap

**Baseline:** [v0.1.0](https://github.com/rockbox37/astriena/releases/tag/v0.1.0) shipped 2026-09-10 — pure sampling engine, ClickHouse BYOS exporter, isolation + head-to-head benchmarks, integration tests (local opt-in), self-metrics + observability docs, GHCR multi-arch images.

**Goal for v0.2.0:** Close the evidence and ops gaps left after v0.1.0 — prove concurrent-ingest wins from lock-striping, harden eviction under cap pressure, and make the project operable in CI and cloud deployments without expanding scope into a policy wishlist or a Rust rewrite.

Tracking issue: see GitHub issue **v0.2.0 roadmap** (label `milestone`).

---

## Themes

| Theme | Focus |
|---|---|
| **Performance** | Measure striping under concurrent load; O(1) pending eviction; optional stripe tuning informed by data |
| **Ops** | ClickHouse integration in CI; importable Grafana dashboard; health/readiness for orchestrators |
| **Product** | One high-value sampler policy; positioning aligned with benchmark evidence; hooks `astriena-cloud` can rely on |

---

## P0 — must ship

### 1. Concurrent ingest benchmark ✅

**Why:** v0.1.0 landed 64-stripe lock striping (#8, #16) specifically for concurrent OTLP ingest, but [`bench/compare_bench_test.go`](../bench/compare_bench_test.go) is single-goroutine. [`docs/benchmarks.md`](benchmarks.md) explicitly notes the h2h bench *cannot exercise* mutex-contention wins.

**Deliverables:**
- `BenchmarkConcurrentConsume/{astriena,stock}` in `bench/` — N worker goroutines, shared processor, same keep policy and workload as existing h2h.
- Document methodology, goroutine counts, and results in `docs/benchmarks.md`.
- Acceptance: reproducible `make bench-h2h-concurrent` (or similar) target; numbers recorded before claiming ingest advantage under load.

**Out of scope:** Customer-scale soak tests, Rust hot path (deferred per [`architecture.md`](architecture.md)).

---

## P1 — should ship

### 2. O(1) pending index for MaxTraces eviction ✅

**Why:** `evictOldestPendingLocked` previously walked the arrival-ordered list front-to-back until it found a `DecisionPending` trace ([`engine.go`](../internal/sampling/engine.go)). When the buffer held many decided-but-not-yet-removed traces at the front (sink retry holding `DecisionSampled`, or traces waiting expiry), eviction degraded toward O(n) per admission under cap pressure.

**Deliverables:**
- Secondary index (`pendingOrder` — pending-only doubly-linked list) so oldest Pending is reachable in O(1). **Shipped.**
- Regression tests for cap pressure with mixed Pending / Sampled-held / decided traces at list head. **Shipped.**
- Re-run isolation + concurrent benches after the change.

### 3. ClickHouse integration tests in CI ✅

**Why:** [`components/clickhouseexporter/integration_test.go`](../components/clickhouseexporter/integration_test.go) and [`docs/clickhouse-integration.md`](clickhouse-integration.md) existed, but [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) ran unit tests only. Regressions in the real driver binding required a maintainer laptop.

**Deliverables:**
- CI job with a ClickHouse service container (or `make clickhouse-up` equivalent). **Shipped** — `clickhouse-integration` in `.github/workflows/ci.yml`.
- `CLICKHOUSE_DSN` wired; `go test -tags=integration` on `clickhouseexporter`. **Shipped** via `make test-integration`.
- Document any flake/retry policy; keep job optional or non-blocking initially if startup is slow, then promote to required once stable. **Partly shipped** — non-blocking via `continue-on-error: true`; the promotion criterion and the absence of a flake/retry policy are recorded in `docs/clickhouse-integration.md`.

### 4. README / positioning alignment with benchmark evidence ✅

**Why:** Head-to-head results showed adapter-level heap at parity with stock (see [`docs/benchmarks.md`](benchmarks.md)), not "a fraction of the memory." The README and architecture header still led with the memory fraction claim, which was retired here; [`docs/benchmarks.md`](benchmarks.md) already documented the honest read.

**Deliverables:**
- Update README and architecture positioning to lead with **ingestion reduction (~80%)** and **ingest allocation cost**, with memory framed as isolation bookkeeping rather than a supported adapter-level ratio. **Shipped** — also covers `builder-config.yaml` and the benchmark comments in `internal/sampling` / `bench/`. Wall-clock ingest was deliberately *not* used as a headline claim: the recorded ns/op figures predate #36 and need re-measuring on an idle machine.
- Remove stale references to `TODO(core)` in engine.go (markers were absorbed into v0.1.0 work). **Shipped** — the last reference was in `docs/benchmarks.md`.

### 5. Health / readiness extension for orchestrators

**Why:** `astriena-cloud` and k8s deployments need a liveness signal independent of `:8888/metrics`. [`builder-config.yaml`](../builder-config.yaml) currently ships `extensions: []`.

**Deliverables:**
- Add contrib `health_check` extension to the distribution.
- Example config snippet in README / observability doc.
- Document scrape vs health ports for BYOS boundary clarity.

### 6. One additional sampler policy — `probabilistic`

**Why:** Only `status_code` and `latency` policies exist ([`policy.go`](../internal/sampling/policy.go)). A probabilistic keep rule is the smallest additive policy that unlocks "keep 100% errors + 1% of healthy traces" style configs without porting the entire contrib policy surface.

**Deliverables:**
- Pure-engine `ProbabilisticPolicy` + config validation in `astrienasampler`.
- Unit tests + parity note in benchmarks doc (does not need h2h parity with stock for v0.2.0).
- **Not in v0.2.0:** `rate_limiting`, `string_attribute`, trace-state policies — defer unless a concrete user request lands before cut.

---

## P2 — nice-to-have (cut if cycle slips)

### 7. Grafana dashboard JSON

**Why:** [`docs/observability.md`](observability.md) lists panel suggestions but no importable artifact.

**Deliverables:** `docs/grafana/astriena-overview.json` (or similar) matching the documented panels — sampler overview, keep ratio, ClickHouse write health, OTLP ingress.

### 8. Configurable stripe count

**Why:** `numStripes = 64` is fixed in [`engine.go`](../internal/sampling/engine.go). Concurrent benchmark results should drive whether this becomes configurable or stays a constant with documented rationale.

**Deliverables:** Only if concurrent bench shows sensitivity — optional `num_stripes` with sane bounds and default 64.

### 9. End-to-end pipeline integration test

**Why:** Exporter integration tests cover the write path; sampler + exporter together under OTLP ingress is untested in CI.

**Deliverables:** Short-lived distro test: OTLP HTTP POST → `astriena_sampler` → `clickhouse` exporter → row visible in ClickHouse. Can share the CI ClickHouse service from item 3.

### 10. Collector line bump

**Why:** Pinned to OTel Collector `v0.160.0` / `v1.66.0`. Worth evaluating one minor bump if security or pdata fixes accumulate — not a goal in itself.

**Deliverables:** Bump `builder-config.yaml` + component `go.mod` files with full CI green; note in CHANGELOG.

---

## Explicitly deferred (not v0.2.0)

| Item | Rationale |
|---|---|
| Rust hot-path rewrite | No evidence Go loses at adapter boundary; see architecture.md |
| Dual pdata/domain representation removal | Large adapter refactor; needs design, not a patch release |
| Full contrib tail-sampling policy parity | Scope explosion; one policy (probabilistic) is enough for v0.2.0 |
| SaaS / cloud control plane code | Lives in private `astriena-cloud`; OSS ships pinned releases only |
| Authentication on `:8888/metrics` | Document reverse-proxy pattern; built-in auth is a product decision |

---

## Suggested sequencing

```
P0 concurrent bench ──▶ informs ──▶ P2 stripe tuning (if needed)
         │
         ▼
P1 pending eviction index
         │
         ├──▶ P1 CI integration tests ──▶ P2 e2e pipeline test
         │
         ├──▶ P1 README alignment (can land anytime)
         │
         ├──▶ P1 health_check extension
         │
         └──▶ P1 probabilistic policy
```

**Recommended first work item:** **P0 — concurrent ingest benchmark.** It validates the main v0.1.0 engineering bet (lock striping), unblocks honest performance messaging, and informs whether P2 stripe tuning is worth doing at all.

---

## Release gate

v0.2.0 tag when:

- [ ] All P0 items complete
- [ ] At least four of six P1 items complete (pending index + CI integration + README alignment are strongly preferred)
- [ ] CHANGELOG `[Unreleased]` → `[0.2.0]` with date
- [ ] `astriena-cloud` pin bump documented upstream (consumer repo, not blocking OSS tag)
