# ClickHouse exporter integration test

The `clickhouseexporter` adapter has unit tests with a fake driver. This document
describes the **integration test** that exercises the real `clickhouse-go/v2`
binding against a live ClickHouse cluster — run automatically in CI (see
[CI](#ci)), and opt-in locally.

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

The `clickhouse-integration` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
runs these tests on pull requests and on pushes to `main`, against a service
container using the same image as `make clickhouse-up`, with
`CLICKHOUSE_DSN=clickhouse://localhost:9000/default`. GitHub holds the job's
steps until the container's `clickhouse-client --query 'SELECT 1'` health check
passes, so the tests never open a connection before the server is serving
queries. The job's `--health-*` options set the probe interval, the per-probe
timeout, the retry budget, and a start period during which early failures do not
count against that budget — so a server that comes up quickly is detected as soon
as its first probe passes, and one that never reports healthy fails the job.

The job is currently **`continue-on-error: true`** — it reports but does not
block a merge. **Promote it once it has run green for a full release cycle
without flaking on service startup.** Two caveats apply when doing so:

- The flag keeps the overall workflow run green even when the job fails, so
  "has it run green for a release cycle" has to be checked by hand in the
  Actions tab.
- Promotion is **two** changes, not one: remove `continue-on-error: true` from
  the job *and* add it to branch protection as a required check. Branch
  protection matches a job's display name, so the check to require is
  `clickhouse exporter (integration, real server)`, not the job id.

There is no flake or retry policy yet; `continue-on-error` is what currently
absorbs a flaky service startup.

`make test` needs no Docker: it does not build the integration-tagged tests at
all, so the build tag — not the in-test skip — is what keeps them out of the
default flow. `make test-integration` does need a reachable server (see Quick
start above); it requires `CLICKHOUSE_DSN` and exits with an error if unset. The
`t.Skip` in the tests is reached only via a direct `go test -tags=integration`.

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
