package astrienasampler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"

	"github.com/rockbox37/astriena/internal/sampling"
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

func makeTraceN(traceID [16]byte, status ptrace.StatusCode, n int) ptrace.Traces {
	return makeTraceRange(traceID, status, 1, n)
}

func makeTraceRange(traceID [16]byte, status ptrace.StatusCode, start, n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID(traceID))
		var sid [8]byte
		sid[0] = byte(start + i)
		sp.SetSpanID(pcommon.SpanID(sid))
		sp.SetName(fmt.Sprintf("op-%d", start+i))
		sp.Status().SetCode(status)
		sp.SetStartTimestamp(0)
		sp.SetEndTimestamp(pcommon.Timestamp(50 * time.Millisecond))
	}
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

func TestFromEngineSpansPreservesRawOnCopy(t *testing.T) {
	spans := toEngineSpans(makeTrace([16]byte{0x01}, ptrace.StatusCodeError, "op"), 0, nil)
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	first := fromEngineSpans(spans)
	if first.SpanCount() != 1 {
		t.Fatalf("first fromEngineSpans: SpanCount=%d, want 1", first.SpanCount())
	}
	// A sink-error retry must see the same snapshot, not an emptied carrier.
	second := fromEngineSpans(spans)
	if second.SpanCount() != 1 {
		t.Fatalf("retry fromEngineSpans: SpanCount=%d, want 1 (Raw must survive copy)", second.SpanCount())
	}
	snap, ok := spans[0].Raw.(ptrace.Traces)
	if !ok || snap.SpanCount() != 1 {
		t.Fatalf("carrier Raw emptied after fromEngineSpans")
	}
}

func TestToEngineSpans_Fields(t *testing.T) {
	spans := toEngineSpans(makeTrace([16]byte{0x01}, ptrace.StatusCodeError, "op"), 0, nil)
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

func TestToEngineSpans_CapsSnapshotAtMaxSpansPerTrace(t *testing.T) {
	tid := [16]byte{0x0A}
	spans := toEngineSpans(makeTraceN(tid, ptrace.StatusCodeError, 5), 2, nil)
	if len(spans) != 5 {
		t.Fatalf("want 5 domain spans so the engine can count overflow, got %d", len(spans))
	}
	snap, ok := spans[0].Raw.(ptrace.Traces)
	if !ok {
		t.Fatalf("carrier Raw = %T, want ptrace.Traces", spans[0].Raw)
	}
	if got := snap.SpanCount(); got != 2 {
		t.Fatalf("snapshot SpanCount = %d, want 2 (cap at copy)", got)
	}
	if fromEngineSpans(spans).SpanCount() != 2 {
		t.Fatalf("emitted SpanCount = %d, want 2 (overflow must not be in Raw)", fromEngineSpans(spans).SpanCount())
	}
	for i := 1; i < len(spans); i++ {
		if spans[i].Raw != nil {
			t.Fatalf("non-carrier spans[%d].Raw = %T, want nil", i, spans[i].Raw)
		}
	}
}

func TestToEngineSpans_CountsEngineHeldTowardCap(t *testing.T) {
	tid := sampling.TraceID{0x0B}
	held := map[sampling.TraceID]int{tid: 1}
	spans := toEngineSpans(makeTraceN([16]byte(tid), ptrace.StatusCodeError, 3), 2, held)
	snap, ok := spans[0].Raw.(ptrace.Traces)
	if !ok {
		t.Fatalf("carrier Raw = %T, want ptrace.Traces", spans[0].Raw)
	}
	if got := snap.SpanCount(); got != 1 {
		t.Fatalf("snapshot SpanCount = %d, want 1 (held 1 + room 1 of cap 2)", got)
	}
}

func TestConsumeTraces_OversizedBatchHonorsMaxSpansPerTrace(t *testing.T) {
	sink := &consumertest.TracesSink{}
	cfg := &Config{
		DecisionWait:     time.Second,
		MaxTraces:        100,
		MaxSpansPerTrace: 2,
		Policies:         []PolicyCfg{{Type: "status_code", Keep: "ERROR"}},
	}
	p, err := newProcessor(processor.Settings{}, cfg, sink)
	if err != nil {
		t.Fatalf("newProcessor: %v", err)
	}

	if err := p.ConsumeTraces(context.Background(), makeTraceN([16]byte{0xCC}, ptrace.StatusCodeError, 4)); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	total := 0
	for _, td := range sink.AllTraces() {
		total += td.SpanCount()
	}
	if total != 2 {
		t.Fatalf("emitted %d spans, want 2 (batch of 4 capped at MaxSpansPerTrace)", total)
	}
	if got := p.engine.Dropped(); got != 2 {
		t.Fatalf("Dropped() = %d, want 2 (in-batch overflow)", got)
	}
}

func TestConsumeTraces_CrossBatchOccupancyCapsSnapshot(t *testing.T) {
	sink := &consumertest.TracesSink{}
	cfg := &Config{
		DecisionWait:     time.Hour,
		MaxTraces:        100,
		MaxSpansPerTrace: 2,
		Policies:         []PolicyCfg{{Type: "status_code", Keep: "ERROR"}},
	}
	p, err := newProcessor(processor.Settings{}, cfg, sink)
	if err != nil {
		t.Fatalf("newProcessor: %v", err)
	}

	tid := [16]byte{0xDD}
	// First batch: one OK span stays Pending and occupies 1 of the cap.
	if err := p.ConsumeTraces(context.Background(), makeTraceRange(tid, ptrace.StatusCodeOk, 1, 1)); err != nil {
		t.Fatalf("first ConsumeTraces: %v", err)
	}
	// Second batch: 3 new ERROR spans. Room left is 1 — only that one may be copied.
	if err := p.ConsumeTraces(context.Background(), makeTraceRange(tid, ptrace.StatusCodeError, 2, 3)); err != nil {
		t.Fatalf("second ConsumeTraces: %v", err)
	}

	total := 0
	for _, td := range sink.AllTraces() {
		total += td.SpanCount()
	}
	if total != 2 {
		t.Fatalf("emitted %d spans, want 2 (1 held + 1 room of cap 2)", total)
	}
	if got := p.engine.Dropped(); got != 2 {
		t.Fatalf("Dropped() = %d, want 2 (second-batch overflow)", got)
	}
}
