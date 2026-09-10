# Observability

Astriena exposes **self-telemetry** — operational metrics about the collector
process itself — via the OpenTelemetry Collector's built-in Prometheus reader.
These metrics describe sampler behavior, ClickHouse write health, and standard
collector pipeline stats. They are **not** customer trace data and are **not**
written to your ClickHouse cluster.

## What to scrape

The example [`config.yaml`](../config.yaml) enables a pull exporter:

```yaml
service:
  telemetry:
    metrics:
      level: detailed
      readers:
        - pull:
            exporter:
              prometheus:
                host: 0.0.0.0
                port: 8888
```

Point Prometheus (or any scraper) at:

```text
http://<astriena-host>:8888/metrics
```

Bind the host/port in your deployment config if `:8888` is not suitable. The
endpoint serves Prometheus text format only — no authentication is built in;
restrict network access or put a reverse proxy in front in production.

### BYOS boundary

Astriena's **Bring Your Own Storage** model means trace spans land in **your**
ClickHouse tables (`spans`, sparse attribute columns, etc.). The `:8888/metrics`
endpoint is a separate, side-channel telemetry surface:

| Surface | Contains | Destination |
|---|---|---|
| OTLP pipeline → `clickhouse` exporter | Sampled trace spans | Your ClickHouse cluster |
| `service.telemetry` Prometheus reader | `astriena_*` and `otelcol_*` counters | Your metrics stack (scrape) |

Do not confuse operational counters with trace volume in ClickHouse SQL. Use
`astriena_sampler_*` to understand sampling decisions and `astriena_clickhouse_*`
to understand write-path health.

## Astriena metric catalog

All `astriena_*` metrics are monotonic counters registered by the custom
components. Values are cumulative for the lifetime of the process (reset on
restart).

### Sampler (`astriena_sampler_*`)

Defined in [`components/astrienasampler/metrics.go`](../components/astrienasampler/metrics.go);
backed by [`internal/sampling`](../internal/sampling) engine counters.

| Metric | Unit | Labels | Meaning | Increments when |
|---|---|---|---|---|
| `astriena_sampler_spans_dropped` | `{span}` | `reason` | Spans dropped by memory safeguards | A span is rejected because a limit was hit. `reason=max_traces`: `MaxTraces` cap reached and no evictable Pending trace remained. `reason=max_spans_per_trace`: a trace exceeded `MaxSpansPerTrace`. |
| `astriena_sampler_traces_not_sampled` | `{trace}` | — | Traces given the default not-sampled decision | Normal ingestion reduction: `DecisionWait` elapsed with no keep policy match, `MaxTraces` pressure evicted the oldest still-Pending trace, or shutdown forced a pending trace to drop. **Not** counted in `spans_dropped`. |
| `astriena_sampler_spans_forwarded` | `{span}` | — | Spans forwarded after a keep decision | A policy chose to keep the trace and the span was emitted to the next pipeline stage. |

`traces_not_sampled` rising is usually **expected** under healthy operation —
it reflects the ~80% ingestion reduction goal. Investigate `spans_dropped`
(with either `reason` label) as a sign of memory-pressure loss.

### ClickHouse writer (`astriena_clickhouse_*`)

Defined in [`components/clickhouseexporter/metrics.go`](../components/clickhouseexporter/metrics.go);
backed by [`internal/clickhouse`](../internal/clickhouse) writer counters.

| Metric | Unit | Meaning | Increments when |
|---|---|---|---|
| `astriena_clickhouse_flush_attempts` | `{flush}` | Flush attempts | The background flush worker starts an insert (batch-size trigger, flush-interval ticker, or `Close` drain). |
| `astriena_clickhouse_flush_failures` | `{flush}` | Flush failures | A flush attempt's `InsertBatch` returns an error (network, auth, schema, ClickHouse overload). |
| `astriena_clickhouse_rows_rebuffered` | `{row}` | Rows re-buffered after a failed flush | A failed batch is put back at the front of the in-memory buffer for retry (counted per row, not per flush). |
| `astriena_clickhouse_writes_rejected_buffer_full` | `{write}` | Writes rejected because the buffer cap was exceeded | An incoming `Write` would exceed the configured buffer cap; the write is rejected with `errBufferFull` and the span is not buffered. |

## Standard `otelcol_*` metrics (brief)

With `level: detailed`, the Collector also exports its usual pipeline metrics.
Worth monitoring in addition to the Astriena-specific counters:

| Metric family | Why it matters |
|---|---|
| `otelcol_receiver_accepted_spans` / `otelcol_receiver_refused_spans` | OTLP ingress health — refused spans mean backpressure or misconfiguration upstream. |
| `otelcol_exporter_sent_spans` / `otelcol_exporter_send_failed_spans` | End-to-end export success from the collector's perspective (includes batch processor output to ClickHouse). |
| `otelcol_processor_batch_*` | Batch processor behavior — batch sizes and timeout-triggered sends under load. |
| `otelcol_process_runtime_total_sys_memory_bytes` | Process RSS trend — complements sampler memory-safeguard drops. |
| `otelcol_process_uptime` | Detect unexpected restarts (counter resets). |

Metric names may include a `{transport}` or `{exporter}` label depending on
component. Filter by `exporter="clickhouse"` when isolating the BYOS write path.

## Suggested alerts

Examples assume Prometheus scraping every 15s. Tune thresholds to your traffic
baseline; start conservative and tighten after a burn-in period.

### Critical — data loss or export broken

```yaml
# Any sustained ClickHouse flush failures
- alert: AstrienaClickHouseFlushFailures
  expr: rate(astriena_clickhouse_flush_failures[5m]) > 0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: ClickHouse flush failures on {{ $labels.instance }}
    description: Flush error rate {{ $value | humanize }}/s for 2m — spans may be re-buffered or backing up.

# Buffer cap rejections — spans dropped at the writer
- alert: AstrienaClickHouseBufferFull
  expr: increase(astriena_clickhouse_writes_rejected_buffer_full[5m]) > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: ClickHouse writer buffer full on {{ $labels.instance }}
    description: {{ $value }} writes rejected in 5m — increase buffer cap or reduce ingress.

# Memory-safeguard span drops (either reason)
- alert: AstrienaSamplerMemoryDrops
  expr: rate(astriena_sampler_spans_dropped[5m]) > 0
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: Sampler dropping spans under memory pressure
    description: Drop rate {{ $value | humanize }}/s — review max_traces / max_spans_per_trace or scale out.

# OTLP receiver refusing spans
- alert: AstrienaReceiverRefusedSpans
  expr: rate(otelcol_receiver_refused_spans[5m]) > 0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: OTLP receiver refusing spans
    description: Refused rate {{ $value | humanize }}/s — upstream or queue saturation.
```

### Warning — degradation or mis-tuning

```yaml
# Re-buffering after failed flushes (may precede critical if ClickHouse is flaky)
- alert: AstrienaClickHouseRowsRebuffered
  expr: rate(astriena_clickhouse_rows_rebuffered[5m]) > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: Rows re-buffered after failed ClickHouse flushes
    description: Re-buffer rate {{ $value | humanize }}/s — check ClickHouse latency and errors.

# High flush failure ratio
- alert: AstrienaClickHouseFlushFailureRatio
  expr: |
    rate(astriena_clickhouse_flush_failures[5m])
    / rate(astriena_clickhouse_flush_attempts[5m]) > 0.05
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: Elevated ClickHouse flush failure ratio (>5%)

# Export failures from collector's view
- alert: AstrienaExporterSendFailed
  expr: rate(otelcol_exporter_send_failed_spans{exporter="clickhouse"}[5m]) > 0
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: Collector reporting failed span exports to ClickHouse

# Process memory climbing (adjust threshold to your deployment size)
- alert: AstrienaHighMemory
  expr: otelcol_process_runtime_total_sys_memory_bytes > 2e9
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: Astriena process memory above 2 GiB
```

`astriena_sampler_traces_not_sampled` is **not** alert-worthy by itself — a
high rate usually means sampling is working. Alert on `spans_dropped` or
`writes_rejected_buffer_full` instead.

## Grafana panel suggestions

- **Sampler overview** — stacked rate of `spans_forwarded`, `traces_not_sampled`
  (as trace rate), and `spans_dropped` split by `reason`.
- **Keep ratio** — `rate(spans_forwarded) / (rate(spans_forwarded) + rate(spans_dropped))`
  over 5m; expect a low ratio if policies are aggressive.
- **ClickHouse write health** — `flush_attempts`, `flush_failures`, and
  `rows_rebuffered` rates on one panel; add `writes_rejected_buffer_full` as a
  stat panel (should stay at zero).
- **Flush success ratio** — `1 - (rate(flush_failures) / rate(flush_attempts))`.
- **OTLP ingress** — `receiver_accepted_spans` vs `receiver_refused_spans`.
- **End-to-end export** — `exporter_sent_spans{exporter="clickhouse"}` vs
  `exporter_send_failed_spans`.
- **Process resources** — `process_runtime_total_sys_memory_bytes` and uptime.

## Related docs

- [Architecture](architecture.md) — pipeline shape and BYOS positioning
- [ClickHouse integration test](clickhouse-integration.md) — exporter self-metrics note
- [`config.yaml`](../config.yaml) — telemetry reader configuration
