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

func TestDecisionWaitDropsPendingTrace(t *testing.T) {
	sink := &recordingSink{}
	base := time.Unix(0, 0)
	clk := base
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)
	eng.now = func() time.Time { return clk }

	tid := TraceID{0x07}
	// A 200 OK span the policy never matches: it stays Pending, buffered.
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: tid, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := eng.NotSampled(); got != 0 {
		t.Fatalf("before DecisionWait: NotSampled() = %d, want 0", got)
	}

	// Advance past DecisionWait and expire: the trace must be decided NotSampled
	// (dropped), not forwarded, and not left buffered.
	clk = base.Add(2 * time.Second)
	eng.expire()
	if len(sink.got) != 0 {
		t.Fatalf("expected nothing forwarded, got %d batches", len(sink.got))
	}
	if got := eng.NotSampled(); got != 1 {
		t.Fatalf("NotSampled() = %d, want 1 (pending trace dropped at DecisionWait)", got)
	}
}

func TestShutdownDrainsPendingAsNotSampled(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Hour, // long enough that nothing expires on its own
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)
	eng.Start()

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x08}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	// Still buffered: the hour-long wait has not elapsed.
	if got := eng.NotSampled(); got != 0 {
		t.Fatalf("before Shutdown: NotSampled() = %d, want 0", got)
	}

	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if len(sink.got) != 0 {
		t.Fatalf("expected nothing forwarded, got %d batches", len(sink.got))
	}
	if got := eng.NotSampled(); got != 1 {
		t.Fatalf("NotSampled() = %d, want 1 (pending trace force-dropped on shutdown)", got)
	}
}

func TestLateErrorSpanSamplesBufferedTrace(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Hour, // long, so nothing expires during the test
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)

	tid := TraceID{0x0a}
	// Batch 1: a healthy span for tid — buffered as Pending.
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: tid, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume 1: %v", err)
	}
	// Batch 2: a different trace entirely. Because Consume evaluates only the
	// traces a batch touches, tid is not re-examined here — it must stay buffered.
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x0b}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume 2: %v", err)
	}
	if len(sink.got) != 0 {
		t.Fatalf("nothing should be sampled yet, got %d batches", len(sink.got))
	}

	// Batch 3: an ERROR span arrives for tid. Now tid is touched, so it is
	// evaluated and sampled — and forwarded with BOTH its spans, proving the
	// buffered span was retained across batches.
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: tid, StatusCode: "ERROR"},
	}); err != nil {
		t.Fatalf("Consume 3: %v", err)
	}
	if len(sink.got) != 1 {
		t.Fatalf("expected 1 sampled trace, got %d", len(sink.got))
	}
	if got := len(sink.got[0]); got != 2 {
		t.Fatalf("sampled trace forwarded %d spans, want 2 (OK + ERROR)", got)
	}
}

func TestBufferIndexAndOrderStayConsistent(t *testing.T) {
	sink := &recordingSink{}
	base := time.Unix(0, 0)
	clk := base
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)
	eng.now = func() time.Time { return clk }

	// A mix: one trace sampled immediately (removed via decide), three left
	// Pending (removed later via expiry). After each step the arrival-ordered
	// list must hold exactly the same traces as the id index — a leaked list node
	// would desync the two and, in production, defeat the O(k) expiry walk.
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x01}, StatusCode: "ERROR"}, // sampled now
		{TraceID: TraceID{0x02}, StatusCode: "OK"},    // pending
		{TraceID: TraceID{0x03}, StatusCode: "OK"},    // pending
		{TraceID: TraceID{0x04}, StatusCode: "OK"},    // pending
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if ln, mn := eng.order.Len(), len(eng.traces); ln != mn {
		t.Fatalf("after consume: order.Len()=%d, len(traces)=%d, want equal", ln, mn)
	}
	if mn := len(eng.traces); mn != 3 {
		t.Fatalf("expected 3 buffered pending traces, got %d", mn)
	}

	// Expire the rest and confirm both structures drain to empty.
	clk = base.Add(2 * time.Second)
	eng.expire()
	if ln, mn := eng.order.Len(), len(eng.traces); ln != 0 || mn != 0 {
		t.Fatalf("after expiry: order.Len()=%d, len(traces)=%d, want 0/0", ln, mn)
	}
	if got := eng.NotSampled(); got != 3 {
		t.Fatalf("NotSampled()=%d, want 3", got)
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
