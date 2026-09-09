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
}

func newProcessor(_ processor.Settings, cfg *Config, next consumer.Traces) (*samplerProcessor, error) {
	eng := sampling.NewEngine(sampling.Config{
		DecisionWait:     cfg.DecisionWait,
		MaxTraces:        cfg.MaxTraces,
		MaxSpansPerTrace: cfg.MaxSpansPerTrace,
		Policies:         buildPolicies(cfg),
	}, &consumerSink{next: next})
	return &samplerProcessor{engine: eng}, nil
}

func (p *samplerProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *samplerProcessor) Start(context.Context, component.Host) error {
	p.engine.Start()
	return nil
}

func (p *samplerProcessor) Shutdown(ctx context.Context) error {
	return p.engine.Shutdown(ctx)
}

// ConsumeTraces translates incoming pdata into engine spans and feeds the engine.
func (p *samplerProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return p.engine.Consume(ctx, toEngineSpans(td))
}

// consumerSink forwards the spans of sampled traces back into the pipeline.
type consumerSink struct{ next consumer.Traces }

func (s *consumerSink) ConsumeSampled(ctx context.Context, spans []*sampling.Span) error {
	td := fromEngineSpans(spans)
	if td.SpanCount() == 0 {
		return nil
	}
	return s.next.ConsumeTraces(ctx, td)
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

// toEngineSpans translates OTLP traces into the engine's framework-free domain
// spans. Only the fields the engine's policies consume (status, duration) are
// populated — the original pdata is preserved in a per-trace snapshot so the
// sampled output keeps every resource, scope, attribute, and span exactly as
// received. Populate more domain fields here only when a policy needs them.
//
// Each source resource/scope block is copied once per trace per batch (not once
// per span), and the snapshot is carried by a single span per trace so the
// reassembly in fromEngineSpans emits it exactly once.
//
// TODO(core): every span in a batch is copied into its trace's snapshot here,
// before the engine applies MaxSpansPerTrace. So the engine's per-trace cap
// bounds cross-batch growth, but the payload copied within one ConsumeTraces
// batch is bounded by the incoming batch size (the OTLP receiver's max message
// size / upstream batch processor), not by MaxSpansPerTrace — and spans past the
// cap that land in the carrier's own batch are still emitted. Enforce the cap
// here, at the copy, to make the bound exact and the drop count precise.
func toEngineSpans(td ptrace.Traces) []*sampling.Span {
	out := make([]*sampling.Span, 0, td.SpanCount())

	// groupKey identifies one (trace, source resource, source scope) within this
	// batch, so spans that share all three land under a single copied block.
	type groupKey struct {
		tid    sampling.TraceID
		ri, si int
	}
	snapByTrace := make(map[sampling.TraceID]ptrace.Traces)
	scopeByGroup := make(map[groupKey]ptrace.ScopeSpans)

	rss := td.ResourceSpans()
	for ri := 0; ri < rss.Len(); ri++ {
		rs := rss.At(ri)
		sss := rs.ScopeSpans()
		for si := 0; si < sss.Len(); si++ {
			ss := sss.At(si)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				tid := sampling.TraceID(sp.TraceID())

				snap, ok := snapByTrace[tid]
				if !ok {
					snap = ptrace.NewTraces()
					snapByTrace[tid] = snap
				}
				key := groupKey{tid: tid, ri: ri, si: si}
				dstScope, ok := scopeByGroup[key]
				if !ok {
					ors := snap.ResourceSpans().AppendEmpty()
					rs.Resource().CopyTo(ors.Resource())
					ors.SetSchemaUrl(rs.SchemaUrl())
					dstScope = ors.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(dstScope.Scope())
					dstScope.SetSchemaUrl(ss.SchemaUrl())
					scopeByGroup[key] = dstScope
				}
				// Copy (not reference): the pipeline may reuse the source pdata
				// once ConsumeTraces returns.
				sp.CopyTo(dstScope.Spans().AppendEmpty())

				out = append(out, &sampling.Span{
					TraceID:    tid,
					SpanID:     sampling.SpanID(sp.SpanID()),
					StatusCode: statusString(sp.Status().Code()),
					DurationNS: durationNS(sp),
				})
			}
		}
	}

	// Attach each trace's snapshot to exactly one of its spans.
	seen := make(map[sampling.TraceID]bool, len(snapByTrace))
	for _, s := range out {
		if !seen[s.TraceID] {
			s.Raw = snapByTrace[s.TraceID]
			seen[s.TraceID] = true
		}
	}
	return out
}

// fromEngineSpans reassembles OTLP traces from the per-trace snapshots carried by
// sampled spans. Non-carrier spans (Raw == nil) are already represented in their
// trace's carrier snapshot, so they are skipped.
//
// Snapshots are copied, not moved: a sink error must leave Raw intact so a
// retry can forward the same spans. MoveAndAppendTo would empty the carrier,
// making the next ConsumeSampled see SpanCount 0 and report success.
func fromEngineSpans(spans []*sampling.Span) ptrace.Traces {
	out := ptrace.NewTraces()
	for _, s := range spans {
		snap, ok := s.Raw.(ptrace.Traces)
		if !ok {
			continue
		}
		rss := snap.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			rss.At(i).CopyTo(out.ResourceSpans().AppendEmpty())
		}
	}
	return out
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
