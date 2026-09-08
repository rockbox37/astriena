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
- Thin Collector adapters: `astriena_sampler` processor and `clickhouse` exporter.
- `builder-config.yaml` (ocb manifest), `Makefile`, example `config.yaml`, and CI
  for the pure core.
- `docs/architecture.md`.
