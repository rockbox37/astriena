package clickhouseexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/rockbox37/astriena/internal/clickhouse"
)

// chExporter wraps the pure clickhouse.Writer for the Collector pipeline.
type chExporter struct {
	w *clickhouse.Writer
}

func newExporter(_ context.Context, _ exporter.Settings, cfg *Config) (exporter.Traces, error) {
	ins, err := newInserter(cfg)
	if err != nil {
		return nil, err
	}
	w, err := clickhouse.NewWriter(clickhouse.Config{
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
	}, ins)
	if err != nil {
		return nil, err
	}
	return &chExporter{w: w}, nil
}

func (e *chExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Start ensures the ClickHouse schema exists and begins the writer's flush loop.
func (e *chExporter) Start(ctx context.Context, _ component.Host) error {
	return e.w.Start(ctx)
}

func (e *chExporter) Shutdown(ctx context.Context) error { return e.w.Close(ctx) }

// ConsumeTraces flattens sampled spans into rows and writes them to ClickHouse.
func (e *chExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	rows := toRows(td)
	if len(rows) == 0 {
		return nil
	}
	return e.w.Write(ctx, rows)
}

// toRows flattens OTLP traces into one row per span for ClickHouse insertion.
func toRows(td ptrace.Traces) []clickhouse.Row {
	rows := make([]clickhouse.Row, 0, td.SpanCount())
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				rows = append(rows, clickhouse.Row{
					Timestamp:  time.Unix(0, int64(sp.StartTimestamp())).UTC(),
					TraceID:    sp.TraceID().String(),
					SpanID:     sp.SpanID().String(),
					Name:       sp.Name(),
					StatusCode: statusString(sp.Status().Code()),
					DurationNS: durationNS(sp),
					Attributes: attrMap(sp.Attributes()),
				})
			}
		}
	}
	return rows
}

func durationNS(sp ptrace.Span) int64 {
	end := sp.EndTimestamp()
	start := sp.StartTimestamp()
	if end < start {
		return 0
	}
	return int64(end - start)
}

func statusString(c ptrace.StatusCode) string {
	switch c {
	case ptrace.StatusCodeError:
		return "ERROR"
	case ptrace.StatusCodeOk:
		return "OK"
	default:
		return ""
	}
}

func attrMap(m pcommon.Map) map[string]string {
	if m.Len() == 0 {
		return nil
	}
	out := make(map[string]string, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		out[k] = v.AsString()
		return true
	})
	return out
}
