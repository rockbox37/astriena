# ClickHouse exporter integration test

The `clickhouseexporter` adapter has unit tests with a fake driver. This document
describes the **opt-in integration test** that exercises the real
`clickhouse-go/v2` binding against a live ClickHouse cluster.

## What it verifies

- Base schema creation (`CREATE DATABASE`, `CREATE TABLE IF NOT EXISTS`)
- Sparse attribute columns via `ALTER TABLE … ADD COLUMN IF NOT EXISTS`
- Bloom-filter data-skipping indexes on sparse columns
- ZSTD-compressed batch insert through `PrepareBatch`
- Column-name collision disambiguation (`a.b` vs `a-b` → distinct `attr_a_b_*` columns)
- Writer async flush worker and time-based flush (`FlushInterval`)

## Prerequisites

- Go 1.26+ (matches `components/clickhouseexporter/go.mod`)
- A reachable ClickHouse native port (default `9000`)

## Quick start (Docker)

```sh
make clickhouse-up
export CLICKHOUSE_DSN=clickhouse://localhost:9000/default
make test-integration
make clickhouse-down
```

`clickhouse-up` binds port 9000 to loopback only and waits until the server
accepts connections.

## Environment

| Variable | Required | Description |
|---|---|---|
| `CLICKHOUSE_DSN` | yes | Native protocol DSN, e.g. `clickhouse://localhost:9000/default` |

Tests create an `astriena_integration` database and unique `spans_it_*` tables
per run, then drop them on cleanup.

## CI

The default CI workflow runs unit tests only (`go test ./...` without the
`integration` build tag). Integration tests are **local/compose opt-in** by
design — no ClickHouse service is required in CI.

## Driver notes (real-cluster observations)

- **ZSTD compression** on the wire (`CompressionZSTD`) works with clickhouse-go/v2
  against server 24.x; no client-side surprises observed.
- **Sparse columns** use `DEFAULT attributes['key']` so inserts stay on the
  fixed seven-column statement; materialized values are queryable immediately.
- **`ADD COLUMN IF NOT EXISTS` / `ADD INDEX IF NOT EXISTS`** are idempotent and
  safe under concurrent flushes (matches Writer's auto-schema contract).
- **Identifier quoting**: attribute keys with dots/dashes are sanitized and
  hashed (`attr_a_b_<8hex>`); distinct raw keys that sanitize to the same base
  always get separate columns.
- **Permissions**: the DSN user needs `CREATE DATABASE`, `CREATE TABLE`, `ALTER
  TABLE`, and `INSERT` on the target database.
- **SQL `LIKE` underscore**: in ClickHouse `LIKE 'attr_%'` treats `_` as a
  single-character wildcard, so it matches the base `attributes` column too.
  Use `startsWith(name, 'attr_')` when listing sparse columns.
- **Database auto-created on connect**: when a target database is configured
  (via `database` or the DSN path), `newInserter` tries to connect directly
  first. If ClickHouse returns error 81 (database missing), it connects via the
  DSN default database, runs `CREATE DATABASE IF NOT EXISTS`, then retries.
  Least-privilege users scoped only to an existing target database are unaffected.

## Self-metrics

ClickHouse writer stats (`astriena_clickhouse_*`) are exported via the Collector's
`service.telemetry` Prometheus reader (see `config.yaml`, port `:8888/metrics`).
They are Astriena operational metrics — not written to your ClickHouse trace table.
