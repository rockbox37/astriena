// Package astrienasampler adapts Astriena's framework-free sampling engine
// (internal/sampling) into an OpenTelemetry Collector traces processor.
//
// The adapter is deliberately thin: it translates pdata <-> the engine's domain
// types and owns no sampling logic. That separation is what keeps the engine
// portable (see docs/architecture.md).
//
// NOTE: this file's OpenTelemetry imports resolve when the distribution is built
// with the OpenTelemetry Collector Builder (`make build`), which pins the
// Collector module versions from builder-config.yaml.
package astrienasampler

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

// Type is the component type used in Collector config ("astriena_sampler").
var Type = component.MustNewType("astriena_sampler")

// NewFactory returns the Collector processor factory for the Astriena sampler.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		DecisionWait:     5 * time.Second,
		MaxTraces:        50_000,
		MaxSpansPerTrace: 10_000,
	}
}

func createTracesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Traces,
) (processor.Traces, error) {
	return newProcessor(set, cfg.(*Config), next)
}
