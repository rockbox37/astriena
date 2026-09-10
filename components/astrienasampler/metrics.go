package astrienasampler

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/rockbox37/astriena/internal/sampling"
)

const meterScope = "github.com/rockbox37/astriena/components/astrienasampler"

type engineMetrics struct {
	droppedSpans     metric.Int64ObservableCounter
	tracesNotSampled metric.Int64ObservableCounter
	spansForwarded   metric.Int64ObservableCounter
	reg              metric.Registration
}

func newEngineMetrics(set component.TelemetrySettings, eng *sampling.Engine) (*engineMetrics, error) {
	meter := set.MeterProvider.Meter(meterScope)

	droppedSpans, err := meter.Int64ObservableCounter(
		"astriena_sampler_spans_dropped",
		metric.WithDescription("Spans dropped by memory safeguards"),
		metric.WithUnit("{span}"),
	)
	if err != nil {
		return nil, err
	}
	tracesNotSampled, err := meter.Int64ObservableCounter(
		"astriena_sampler_traces_not_sampled",
		metric.WithDescription("Traces given the default not-sampled decision"),
		metric.WithUnit("{trace}"),
	)
	if err != nil {
		return nil, err
	}
	spansForwarded, err := meter.Int64ObservableCounter(
		"astriena_sampler_spans_forwarded",
		metric.WithDescription("Spans forwarded after a keep decision"),
		metric.WithUnit("{span}"),
	)
	if err != nil {
		return nil, err
	}

	em := &engineMetrics{
		droppedSpans:     droppedSpans,
		tracesNotSampled: tracesNotSampled,
		spansForwarded:   spansForwarded,
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := eng.Stats()
		o.ObserveInt64(droppedSpans, st.DroppedMaxTraces, metric.WithAttributes(
			attribute.String("reason", "max_traces"),
		))
		o.ObserveInt64(droppedSpans, st.DroppedMaxSpansPerTrace, metric.WithAttributes(
			attribute.String("reason", "max_spans_per_trace"),
		))
		o.ObserveInt64(tracesNotSampled, st.NotSampled)
		o.ObserveInt64(spansForwarded, st.SampledSpans)
		return nil
	}, droppedSpans, tracesNotSampled, spansForwarded)
	if err != nil {
		return nil, err
	}
	em.reg = reg
	return em, nil
}

func (m *engineMetrics) shutdown() error {
	if m == nil || m.reg == nil {
		return nil
	}
	if err := m.reg.Unregister(); err != nil {
		return err
	}
	m.reg = nil
	return nil
}
