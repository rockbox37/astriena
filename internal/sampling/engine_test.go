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
