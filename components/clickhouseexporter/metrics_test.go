package clickhouseexporter

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/rockbox37/astriena/internal/clickhouse"
)

type testInserter struct {
	failInsert error
}

func (f *testInserter) EnsureBaseSchema(context.Context) error { return nil }

func (f *testInserter) AddColumns(context.Context, []string) error { return nil }

func (f *testInserter) InsertBatch(context.Context, []clickhouse.Row) error {
	fail := f.failInsert
	if f.failInsert != nil {
		f.failInsert = nil
	}
	return fail
}

func (f *testInserter) Close() error { return nil }

func waitFlushAttempts(t *testing.T, w *clickhouse.Writer, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.Stats().FlushAttempts >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("FlushAttempts = %d, want >= %d", w.Stats().FlushAttempts, want)
}

func newTestWriterMetrics(t *testing.T) (*componenttest.Telemetry, *clickhouse.Writer, *testInserter) {
	t.Helper()
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() {
		_ = tel.Shutdown(context.Background())
	})

	ins := &testInserter{}
	w, err := clickhouse.NewWriter(clickhouse.Config{BatchSize: 2}, ins)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() {
		_ = w.Close(context.Background())
	})

	metrics, err := newWriterMetrics(tel.NewTelemetrySettings(), w)
	if err != nil {
		t.Fatalf("newWriterMetrics: %v", err)
	}
	t.Cleanup(func() {
		_ = metrics.shutdown()
	})
	return tel, w, ins
}

func TestMetricsReportFlushCounters(t *testing.T) {
	tel, w, ins := newTestWriterMetrics(t)
	ctx := context.Background()

	ins.failInsert = errors.New("clickhouse unavailable")
	if err := w.Write(ctx, []clickhouse.Row{{}, {}}); err != nil {
		t.Fatalf("Write failed flush: %v", err)
	}
	waitFlushAttempts(t, w, 1)

	attempts, err := sumMetric(t, tel, "astriena_clickhouse_flush_attempts")
	if err != nil {
		t.Fatalf("flush_attempts metric: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("astriena_clickhouse_flush_attempts = %d, want 1", attempts)
	}

	failures, err := sumMetric(t, tel, "astriena_clickhouse_flush_failures")
	if err != nil {
		t.Fatalf("flush_failures metric: %v", err)
	}
	if failures != 1 {
		t.Fatalf("astriena_clickhouse_flush_failures = %d, want 1", failures)
	}

	rebuffered, err := sumMetric(t, tel, "astriena_clickhouse_rows_rebuffered")
	if err != nil {
		t.Fatalf("rows_rebuffered metric: %v", err)
	}
	if rebuffered != 2 {
		t.Fatalf("astriena_clickhouse_rows_rebuffered = %d, want 2", rebuffered)
	}
}

func TestMetricsReportBufferFullRejection(t *testing.T) {
	tel, w, ins := newTestWriterMetrics(t)
	ctx := context.Background()

	ins.failInsert = errors.New("clickhouse unavailable")
	if err := w.Write(ctx, []clickhouse.Row{{}, {}}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	waitFlushAttempts(t, w, 1)

	ins.failInsert = errors.New("still down")
	for i := 0; i < 3; i++ {
		want := w.Stats().FlushAttempts + 1
		if err := w.Write(ctx, []clickhouse.Row{{}, {}}); err != nil {
			t.Fatalf("Write %d: %v", i+2, err)
		}
		waitFlushAttempts(t, w, want)
		ins.failInsert = errors.New("still down")
	}
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []clickhouse.Row{{}}); err == nil {
		t.Fatal("Write past cap: expected buffer full error")
	}
	if w.Stats().WritesRejectedBufferFull != 1 {
		t.Fatalf("WritesRejectedBufferFull = %d, want 1", w.Stats().WritesRejectedBufferFull)
	}

	rejected, err := sumMetric(t, tel, "astriena_clickhouse_writes_rejected_buffer_full")
	if err != nil {
		t.Fatalf("writes_rejected_buffer_full metric: %v", err)
	}
	if rejected != 1 {
		t.Fatalf("astriena_clickhouse_writes_rejected_buffer_full = %d, want 1", rejected)
	}
}

func sumMetric(t *testing.T, tel *componenttest.Telemetry, name string) (int64, error) {
	t.Helper()
	m, err := tel.GetMetric(name)
	if err != nil {
		return 0, err
	}
	return sumDataPoints(m)
}

func sumDataPoints(m metricdata.Metrics) (int64, error) {
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		var total int64
		for _, dp := range data.DataPoints {
			total += dp.Value
		}
		return total, nil
	default:
		return 0, nil
	}
}
