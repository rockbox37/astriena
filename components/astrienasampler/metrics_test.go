package astrienasampler

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricsReportEngineCounters(t *testing.T) {
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() {
		_ = tel.Shutdown(context.Background())
	})

	set := processor.Settings{
		ID:                component.NewID(Type),
		TelemetrySettings: tel.NewTelemetrySettings(),
	}
	sink := &consumertest.TracesSink{}
	cfg := &Config{
		DecisionWait:     time.Hour,
		MaxTraces:        100,
		MaxSpansPerTrace: 2,
		Policies:         []PolicyCfg{{Type: "status_code", Keep: "ERROR"}},
	}
	p, err := newProcessor(set, cfg, sink)
	if err != nil {
		t.Fatalf("newProcessor: %v", err)
	}

	if err := p.ConsumeTraces(context.Background(), makeTraceN([16]byte{0xEE}, ptrace.StatusCodeError, 4)); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	droppedMaxSpans, err := droppedByReason(t, tel, "max_spans_per_trace")
	if err != nil {
		t.Fatalf("max_spans_per_trace metric: %v", err)
	}
	if droppedMaxSpans != 2 {
		t.Fatalf("astriena_sampler_spans_dropped{reason=max_spans_per_trace} = %d, want 2", droppedMaxSpans)
	}

	forwarded, err := sumMetric(t, tel, "astriena_sampler_spans_forwarded")
	if err != nil {
		t.Fatalf("spans_forwarded metric: %v", err)
	}
	if forwarded != 2 {
		t.Fatalf("astriena_sampler_spans_forwarded = %d, want 2", forwarded)
	}
}

func droppedByReason(t *testing.T, tel *componenttest.Telemetry, reason string) (int64, error) {
	t.Helper()
	m, err := tel.GetMetric("astriena_sampler_spans_dropped")
	if err != nil {
		return 0, err
	}
	sum, err := sumDataPoints(m, attribute.String("reason", reason))
	if err != nil {
		return 0, err
	}
	return sum, nil
}

func sumMetric(t *testing.T, tel *componenttest.Telemetry, name string) (int64, error) {
	t.Helper()
	m, err := tel.GetMetric(name)
	if err != nil {
		return 0, err
	}
	return sumDataPoints(m)
}

func sumDataPoints(m metricdata.Metrics, attrs ...attribute.KeyValue) (int64, error) {
	sum := m.Data
	switch data := sum.(type) {
	case metricdata.Sum[int64]:
		var total int64
		for _, dp := range data.DataPoints {
			if len(attrs) > 0 && !hasAttributes(dp.Attributes, attrs...) {
				continue
			}
			total += dp.Value
		}
		return total, nil
	default:
		return 0, nil
	}
}

func hasAttributes(set attribute.Set, want ...attribute.KeyValue) bool {
	for _, kv := range want {
		v, ok := set.Value(kv.Key)
		if !ok || v != kv.Value {
			return false
		}
	}
	return true
}
