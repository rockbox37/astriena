package sampling

import (
	"context"
	"testing"
	"time"
)

// recordingSink captures spans forwarded for sampled traces.
type recordingSink struct{ got [][]*Span }

func (r *recordingSink) ConsumeSampled(_ context.Context, spans []*Span) error {
	r.got = append(r.got, spans)
	return nil
}

func TestStatusCodePolicyKeepsErrors(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)

	tid := TraceID{0x01}
	err := eng.Consume(context.Background(), []*Span{
		{TraceID: tid, Name: "GET /checkout", StatusCode: "ERROR"},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(sink.got) != 1 {
		t.Fatalf("expected 1 sampled trace, got %d", len(sink.got))
	}
}

func TestMaxTracesBoundsBuffer(t *testing.T) {
	sink := &recordingSink{}
	// Policy never matches these spans, so both traces would otherwise stay
	// buffered as Pending. MaxTraces=1 must admit the first and drop the second.
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    1,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)

	err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x01}, StatusCode: "OK"},
		{TraceID: TraceID{0x02}, StatusCode: "OK"},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := eng.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d, want 1 (second trace dropped at cap)", got)
	}
}

func TestMaxSpansPerTraceBoundsSingleTrace(t *testing.T) {
	sink := &recordingSink{}
	// One trace id streaming many non-matching spans must not grow without bound.
	eng := NewEngine(Config{
		DecisionWait:     time.Second,
		MaxTraces:        100,
		MaxSpansPerTrace: 2,
		Policies:         []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)

	tid := TraceID{0x09}
	spans := []*Span{
		{TraceID: tid, StatusCode: "OK"},
		{TraceID: tid, StatusCode: "OK"},
		{TraceID: tid, StatusCode: "OK"},
		{TraceID: tid, StatusCode: "OK"},
	}
	if err := eng.Consume(context.Background(), spans); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	// 2 buffered, 2 dropped past the per-trace cap.
	if got := eng.Dropped(); got != 2 {
		t.Fatalf("Dropped() = %d, want 2 (spans past MaxSpansPerTrace)", got)
	}
}

func TestLatencyPolicyDropsFastTrace(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{LatencyPolicy{ThresholdNS: int64(200 * time.Millisecond)}},
	}, sink)

	tid := TraceID{0x02}
	// 50ms span: below the 200ms threshold, so the policy stays Pending and
	// nothing is forwarded yet.
	err := eng.Consume(context.Background(), []*Span{
		{TraceID: tid, Name: "GET /healthz", DurationNS: int64(50 * time.Millisecond)},
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(sink.got) != 0 {
		t.Fatalf("expected no sampled traces, got %d", len(sink.got))
	}
}
