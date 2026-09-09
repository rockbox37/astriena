# Changelog

All notable changes to Astriena are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project uses
[Semantic Versioning](https://semver.org/). Released versions are git tags
(`vMAJOR.MINOR.PATCH`); the private `astriena-cloud` control plane consumes
pinned released versions.

## [Unreleased]

### Added
- Project scaffolding: custom OpenTelemetry Collector distribution shape.
- Pure, framework-free core: `internal/sampling` (tail-sampling engine with
  status-code and latency policies) and `internal/clickhouse` (BYOS writer).
- Thin Collector adapters: `astriena_sampler` processor and `clickhouse` exporter,
  with full pdata <-> domain translation (resource/scope preserved via per-span
  snapshots) and unit tests.
- `builder-config.yaml` (ocb manifest) pinned to the OpenTelemetry Collector
  `v0.160.0` / `v1.66.0` release line; `make build` produces a runnable
  `./_build/astriena`.
- `Makefile`, example `config.yaml`, and CI covering the pure core, the component
  adapters, and the full distribution build.
- `docs/architecture.md`.
