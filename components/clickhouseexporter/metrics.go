package clickhouseexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"

	"github.com/rockbox37/astriena/internal/clickhouse"
)

const meterScope = "github.com/rockbox37/astriena/components/clickhouseexporter"

type writerMetrics struct {
	flushAttempts            metric.Int64ObservableCounter
	flushFailures            metric.Int64ObservableCounter
	rowsRebuffered           metric.Int64ObservableCounter
	writesRejectedBufferFull metric.Int64ObservableCounter
	reg                      metric.Registration
}

func newWriterMetrics(set component.TelemetrySettings, w *clickhouse.Writer) (*writerMetrics, error) {
	meter := set.MeterProvider.Meter(meterScope)

	flushAttempts, err := meter.Int64ObservableCounter(
		"astriena_clickhouse_flush_attempts",
		metric.WithDescription("ClickHouse flush attempts"),
		metric.WithUnit("{flush}"),
	)
	if err != nil {
		return nil, err
	}
	flushFailures, err := meter.Int64ObservableCounter(
		"astriena_clickhouse_flush_failures",
		metric.WithDescription("ClickHouse flush failures"),
		metric.WithUnit("{flush}"),
	)
	if err != nil {
		return nil, err
	}
	rowsRebuffered, err := meter.Int64ObservableCounter(
		"astriena_clickhouse_rows_rebuffered",
		metric.WithDescription("Rows re-buffered after a failed flush"),
		metric.WithUnit("{row}"),
	)
	if err != nil {
		return nil, err
	}
	writesRejectedBufferFull, err := meter.Int64ObservableCounter(
		"astriena_clickhouse_writes_rejected_buffer_full",
		metric.WithDescription("Writes rejected because the buffer cap was exceeded"),
		metric.WithUnit("{write}"),
	)
	if err != nil {
		return nil, err
	}

	wm := &writerMetrics{
		flushAttempts:            flushAttempts,
		flushFailures:            flushFailures,
		rowsRebuffered:           rowsRebuffered,
		writesRejectedBufferFull: writesRejectedBufferFull,
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := w.Stats()
		o.ObserveInt64(flushAttempts, int64(st.FlushAttempts))
		o.ObserveInt64(flushFailures, int64(st.FlushFailures))
		o.ObserveInt64(rowsRebuffered, int64(st.RowsRebuffered))
		o.ObserveInt64(writesRejectedBufferFull, int64(st.WritesRejectedBufferFull))
		return nil
	}, flushAttempts, flushFailures, rowsRebuffered, writesRejectedBufferFull)
	if err != nil {
		return nil, err
	}
	wm.reg = reg
	return wm, nil
}

func (m *writerMetrics) shutdown() error {
	if m == nil || m.reg == nil {
		return nil
	}
	if err := m.reg.Unregister(); err != nil {
		return err
	}
	m.reg = nil
	return nil
}
