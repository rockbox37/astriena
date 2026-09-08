// Package clickhouseexporter adapts Astriena's framework-free ClickHouse writer
// (internal/clickhouse) into an OpenTelemetry Collector traces exporter.
//
// Thin by design: it translates pdata into clickhouse.Row and delegates the
// write path (batching, schema management) to the pure package.
//
// NOTE: OpenTelemetry imports resolve when built with the Collector Builder
// (`make build`); see builder-config.yaml.
package clickhouseexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

// Type is the component type used in Collector config ("clickhouse").
var Type = component.MustNewType("clickhouse")

// NewFactory returns the Collector exporter factory for ClickHouse.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Database:      "astriena",
		Table:         "spans",
		BatchSize:     10_000,
		FlushInterval: 5 * time.Second,
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	return newExporter(ctx, set, cfg.(*Config))
}
