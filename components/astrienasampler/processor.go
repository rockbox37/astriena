package astrienasampler

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"

	"github.com/rockbox37/astriena/internal/sampling"
)

// samplerProcessor wires the pure sampling.Engine into the Collector pipeline.
type samplerProcessor struct {
	engine *sampling.Engine
	next   consumer.Traces
}

func newProcessor(_ processor.Settings, cfg *Config, next consumer.Traces) (*samplerProcessor, error) {
	sink := &consumerSink{next: next}
	eng := sampling.NewEngine(sampling.Config{
		DecisionWait: cfg.DecisionWait,
		MaxTraces:    cfg.MaxTraces,
		Policies:     buildPolicies(cfg),
	}, sink)
	return &samplerProcessor{engine: eng, next: next}, nil
}

func (p *samplerProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *samplerProcessor) Start(context.Context, component.Host) error { return nil }
func (p *samplerProcessor) Shutdown(context.Context) error              { return nil }

// ConsumeTraces translates incoming pdata into engine spans and feeds the engine.
func (p *samplerProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return p.engine.Consume(ctx, toEngineSpans(td))
}

// consumerSink forwards the spans of sampled traces back into the pipeline.
type consumerSink struct{ next consumer.Traces }

func (s *consumerSink) ConsumeSampled(ctx context.Context, spans []*sampling.Span) error {
	return s.next.ConsumeTraces(ctx, fromEngineSpans(spans))
}

func buildPolicies(cfg *Config) []sampling.Policy {
	var out []sampling.Policy
	for _, pc := range cfg.Policies {
		switch pc.Type {
		case "status_code":
			out = append(out, sampling.StatusCodePolicy{Keep: pc.Keep})
		case "latency":
			out = append(out, sampling.LatencyPolicy{ThresholdNS: pc.ThresholdMS * int64(time.Millisecond)})
		}
	}
	return out
}

// toEngineSpans / fromEngineSpans are the pdata <-> domain translation boundary.
// TODO(core): implement. Keeping translation here (not in internal/sampling) is
// what keeps the engine free of OTLP types.
func toEngineSpans(ptrace.Traces) []*sampling.Span { return nil }

func fromEngineSpans([]*sampling.Span) ptrace.Traces { return ptrace.NewTraces() }
