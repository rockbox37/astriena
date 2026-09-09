package astrienasampler

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
)

func makeTrace(traceID [16]byte, status ptrace.StatusCode, name string) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	sp := ss.Spans().AppendEmpty()
	sp.SetTraceID(pcommon.TraceID(traceID))
	sp.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	sp.SetName(name)
	sp.Status().SetCode(status)
	sp.SetStartTimestamp(0)
	sp.SetEndTimestamp(pcommon.Timestamp(50 * time.Millisecond))
	return td
}

// End to end: pdata in -> translate -> engine decision -> reassemble pdata out.
func TestConsumeTraces_KeepsErrorDefersOK(t *testing.T) {
	sink := &consumertest.TracesSink{}
	cfg := &Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []PolicyCfg{{Type: "status_code", Keep: "ERROR"}},
	}
	p, err := newProcessor(processor.Settings{}, cfg, sink)
	if err != nil {
		t.Fatalf("newProcessor: %v", err)
	}

	errTrace := makeTrace([16]byte{0xAA}, ptrace.StatusCodeError, "GET /checkout")
	okTrace := makeTrace([16]byte{0xBB}, ptrace.StatusCodeOk, "GET /healthz")

	if err := p.ConsumeTraces(context.Background(), errTrace); err != nil {
		t.Fatalf("consume err trace: %v", err)
	}
	if err := p.ConsumeTraces(context.Background(), okTrace); err != nil {
		t.Fatalf("consume ok trace: %v", err)
	}

	total := 0
	for _, td := range sink.AllTraces() {
		total += td.SpanCount()
	}
	if total != 1 {
		t.Fatalf("expected 1 sampled span (error trace kept, ok trace deferred), got %d", total)
	}

	out := sink.AllTraces()[0]
	sp := out.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if sp.Name() != "GET /checkout" {
		t.Errorf("forwarded span name = %q, want GET /checkout", sp.Name())
	}
	// The resource must survive the domain round trip via the pdata snapshot.
	svc, ok := out.ResourceSpans().At(0).Resource().Attributes().Get("service.name")
	if !ok || svc.AsString() != "checkout" {
		t.Errorf("resource attribute not preserved through sampling")
	}
}

func TestToEngineSpans_Fields(t *testing.T) {
	spans := toEngineSpans(makeTrace([16]byte{0x01}, ptrace.StatusCodeError, "op"))
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	s := spans[0]
	// Only policy-consumed fields are populated on the domain span; the rest of
	// the span data lives in the pdata snapshot carried in Raw.
	if s.StatusCode != "ERROR" {
		t.Errorf("StatusCode = %q, want ERROR", s.StatusCode)
	}
	if s.DurationNS != int64(50*time.Millisecond) {
		t.Errorf("DurationNS = %d, want %d", s.DurationNS, int64(50*time.Millisecond))
	}
	snap, ok := s.Raw.(ptrace.Traces)
	if !ok {
		t.Fatalf("Raw = %T, want ptrace.Traces (carrier span)", s.Raw)
	}
	if got := snap.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name(); got != "op" {
		t.Errorf("snapshot span name = %q, want op", got)
	}
}
