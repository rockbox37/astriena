package clickhouseexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/rockbox37/astriena/internal/clickhouse"
)

// chExporter wraps the pure clickhouse.Writer for the Collector pipeline.
type chExporter struct {
	w *clickhouse.Writer
}

func newExporter(_ context.Context, _ exporter.Settings, cfg *Config) (exporter.Traces, error) {
	w, err := clickhouse.NewWriter(clickhouse.Config{
		DSN:           cfg.DSN,
		Database:      cfg.Database,
		Table:         cfg.Table,
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
	})
	if err != nil {
		return nil, err
	}
	return &chExporter{w: w}, nil
}

func (e *chExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *chExporter) Start(context.Context, component.Host) error { return nil }
func (e *chExporter) Shutdown(context.Context) error              { return e.w.Close() }

// ConsumeTraces flattens sampled spans into rows and writes them to ClickHouse.
func (e *chExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return e.w.Write(ctx, toRows(td))
}

// toRows is the pdata -> row translation boundary.
// TODO(core): implement, including attribute flattening for AutoSchema.
func toRows(ptrace.Traces) []clickhouse.Row { return nil }
