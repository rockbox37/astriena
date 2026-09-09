# Changelog

All notable changes to Astriena are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project uses
[Semantic Versioning](https://semver.org/). Released versions are git tags
(`vMAJOR.MINOR.PATCH`); the private `astriena-cloud` control plane consumes
pinned released versions.

## [Unreleased]

### Changed
- Redrew the constellation mark as an irregular, mixed-magnitude night sky with
  faint broken chart lines; the original five-star A was too dominant
  (follow-up to #9).

### Fixed
- Keep-worthy traces and accepted ClickHouse rows survive sink errors and
  cancelled Collector shutdown: sampled traces stay held until forward
  succeeds, pdata snapshots are copied rather than moved, `Close`/`Shutdown`
  drain with a live context, and `quoteString` / ParseDSN no longer leak a
  DSN or allow attribute-key breakout in ALTER literals.
- ClickHouse prepared-batch `Abort` errors on a failed append are joined
  into the returned error instead of being discarded.
- `Writer.Close` always closes the Inserter even when the final drain flush
  fails, so a shutdown insert error cannot leak the ClickHouse connection.

### Changed
- Sampling engine hot path is no longer O(n²) in buffer size. `Consume` now
  re-evaluates only the traces a batch touched (policies are pure functions of a
  trace's spans, so an untouched trace cannot change decision) instead of scanning
  the whole buffer, and expiry walks a new arrival-ordered FIFO list — deadline
  order, since all traces share one `DecisionWait` — popping only what is due
  (O(k)) instead of ranging every buffered trace. A per-batch fill that could not
  finish 200k traces in 120s now does 1M in ~2.9s. The background ticker does
  expiry only (it can no longer touch the sink or fail), and traces are unlinked
  in O(1) via a per-trace `*list.Element`. Cost: one extra allocation per new
  trace (4 → 5 allocs/op). Covered by new regression tests
  (`TestLateErrorSpanSamplesBufferedTrace`, `TestBufferIndexAndOrderStayConsistent`).

### Added
- Constellation branding: original asterism mark and night-sky token
  set in `docs/assets/`, wired into the README and docs headers (closes #9).
- Project scaffolding: custom OpenTelemetry Collector distribution shape.
- Pure, framework-free core: `internal/sampling` (tail-sampling engine with
  status-code and latency policies) and `internal/clickhouse` (BYOS writer).
- Thin Collector adapters: `astriena_sampler` processor and `clickhouse` exporter,
  with full pdata <-> domain translation (resource/scope preserved via per-span
  snapshots) and unit tests.
- `DecisionWait` enforcement in the sampling engine: a trace no policy keeps is
  now given the default `NotSampled` decision and dropped once the wait elapses —
  driven inline on ingest and by a background ticker so buffered traces still
  flush when traffic goes quiet — instead of being buffered until `MaxTraces`
  evicts new traces. This is the path that realizes the ingestion reduction.
  Exposed via `Engine.Start`/`Shutdown` (wired to the processor lifecycle) and a
  `NotSampled()` counter distinct from the memory-safeguard `Dropped()` count.
- ClickHouse BYOS write path. `internal/clickhouse` now owns the real batching
  and auto-schema logic behind an `Inserter` port — size/interval/Close-triggered
  flushes, new-attribute-key diffing, and writer-owned re-buffer-on-failure —
  kept pure and unit-tested offline with a fake driver. The clickhouse-go/v2 binding lives in
  the `clickhouseexporter` adapter (`driver.go`): ZSTD-compressed prepared-batch
  inserts, base-schema creation, and per-attribute sparse columns
  (`DEFAULT attributes['k']`) with bloom-filter data-skipping indexes. The root
  module stays dependency-free so the pure core still builds offline. Exporter
  `Validate` now requires a DSN and rejects negative batch settings.
- `builder-config.yaml` (ocb manifest) pinned to the OpenTelemetry Collector
  `v0.160.0` / `v1.66.0` release line; `make build` produces a runnable
  `./_build/astriena`.
- `Makefile`, example `config.yaml`, and CI covering the pure core, the component
  adapters, and the full distribution build.
- Isolation benchmarks for the sampling engine
  (`internal/sampling/bench_test.go`, `make bench`) establishing the baseline the
  wedge claims rest on: the realized drop rate (**79.99%** at a 20% keep fraction,
  proving the "~80% ingestion reduction" through the real policy + `DecisionWait`
  path) and per-trace buffer bookkeeping (~120–180 heap B/trace). Methodology,
  results, and the still-open head-to-head against the stock `tail_sampling`
  processor are in `docs/benchmarks.md`. Writing them surfaced the O(n²)
  buffer-scan since fixed (see Changed above).
- `docs/architecture.md`.
