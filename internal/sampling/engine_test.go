package sampling

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errSink = errors.New("sink unavailable")

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
	// buffered as Pending. MaxTraces=1 must evict the oldest Pending trace and
	// admit the second rather than dropping the incoming span.
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
	if got := eng.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d, want 0 (oldest Pending evicted, not incoming span)", got)
	}
	if got := eng.NotSampled(); got != 1 {
		t.Fatalf("NotSampled() = %d, want 1 (first trace evicted at cap)", got)
	}
	orderLen, traceLen := eng.bufferStateForTest()
	if orderLen != 1 || traceLen != 1 {
		t.Fatalf("buffer state order=%d traces=%d, want 1/1 (second trace retained)", orderLen, traceLen)
	}
}

func TestDroppedSplitByReason(t *testing.T) {
	eng := NewEngine(Config{
		DecisionWait:     time.Hour,
		MaxTraces:        1,
		MaxSpansPerTrace: 1,
		Policies:         []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x31}, StatusCode: "ERROR"},
	}); err == nil {
		t.Fatal("expected sink error")
	}
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x32}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume new trace at MaxTraces: %v", err)
	}
	if got := eng.DroppedMaxTraces(); got != 1 {
		t.Fatalf("DroppedMaxTraces() = %d, want 1", got)
	}
	if got := eng.DroppedMaxSpansPerTrace(); got != 0 {
		t.Fatalf("DroppedMaxSpansPerTrace() = %d, want 0", got)
	}
	if got := eng.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d, want sum of split counters", got)
	}

	eng2 := NewEngine(Config{
		DecisionWait:     time.Second,
		MaxTraces:        100,
		MaxSpansPerTrace: 1,
		Policies:         []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, &recordingSink{})
	tid := TraceID{0x33}
	if err := eng2.Consume(context.Background(), []*Span{
		{TraceID: tid, StatusCode: "OK"},
		{TraceID: tid, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume per-trace overflow: %v", err)
	}
	if got := eng2.DroppedMaxSpansPerTrace(); got != 1 {
		t.Fatalf("DroppedMaxSpansPerTrace() = %d, want 1", got)
	}
	if got := eng2.DroppedMaxTraces(); got != 0 {
		t.Fatalf("DroppedMaxTraces() = %d, want 0", got)
	}
}

func TestSampledSpansCountsForwarded(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x41}, StatusCode: "ERROR"},
		{TraceID: TraceID{0x41}, StatusCode: "ERROR"},
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := eng.SampledSpans(); got != 2 {
		t.Fatalf("SampledSpans() = %d, want 2", got)
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

func TestBufferedSpanCounts(t *testing.T) {
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, &recordingSink{})

	tid := TraceID{0x0A}
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: tid, SpanID: SpanID{0x01}, StatusCode: "OK"},
		{TraceID: tid, SpanID: SpanID{0x02}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got := eng.BufferedSpanCounts([]TraceID{tid, {0x0B}, tid})
	if got[tid] != 2 {
		t.Fatalf("BufferedSpanCounts(%x) = %d, want 2", tid, got[tid])
	}
	if _, ok := got[TraceID{0x0B}]; ok {
		t.Fatalf("missing id must be omitted from occupancy map")
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
	if ln, mn := eng.bufferStateForTest(); ln != mn {
		t.Fatalf("after consume: order.Len()=%d, len(traces)=%d, want equal", ln, mn)
	}
	if _, mn := eng.bufferStateForTest(); mn != 3 {
		t.Fatalf("expected 3 buffered pending traces, got %d", mn)
	}

	// Expire the rest and confirm both structures drain to empty.
	clk = base.Add(2 * time.Second)
	eng.expire()
	if ln, mn := eng.bufferStateForTest(); ln != 0 || mn != 0 {
		t.Fatalf("after expiry: order.Len()=%d, len(traces)=%d, want 0/0", ln, mn)
	}
	if got := eng.NotSampled(); got != 3 {
		t.Fatalf("NotSampled()=%d, want 3", got)
	}
}

func TestConsumeDecisionWaitIgnoresSinkIO(t *testing.T) {
	base := time.Unix(0, 0)
	clk := base
	sink := &recordingSink{}
	// A sink that advances the clock as if ConsumeSampled blocked for longer
	// than DecisionWait. Expiry must still use the Consume-start clock so a
	// sibling pending trace created in the same batch is not default-dropped.
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)
	eng.now = func() time.Time { return clk }
	eng.sink = slowClockSink{recordingSink: sink, advance: func() { clk = base.Add(2 * time.Second) }}

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x11}, StatusCode: "ERROR"},
		{TraceID: TraceID{0x12}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(sink.got) != 1 {
		t.Fatalf("expected the ERROR trace to be sampled, got %d", len(sink.got))
	}
	if got := eng.NotSampled(); got != 0 {
		t.Fatalf("NotSampled() = %d, want 0 (pending sibling must survive sink I/O)", got)
	}
	if counts := eng.BufferedSpanCounts([]TraceID{{0x12}}); counts[TraceID{0x12}] != 1 {
		t.Fatalf("pending sibling was expired during sink I/O")
	}
}

type slowClockSink struct {
	*recordingSink
	advance func()
}

func (s slowClockSink) ConsumeSampled(ctx context.Context, spans []*Span) error {
	s.advance()
	return s.recordingSink.ConsumeSampled(ctx, spans)
}

func TestShutdownCancelledStillDrainsAndIsIdempotent(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)
	eng.Start()
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x13}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Cancelled wait must still drain; a second Shutdown must not panic.
	_ = eng.Shutdown(ctx)
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if got := eng.NotSampled(); got != 1 {
		t.Fatalf("NotSampled() = %d, want 1 after cancelled-then-repeat Shutdown", got)
	}
}

func TestTouchedDoesNotRetainDecidedTraces(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x14}, StatusCode: "ERROR"},
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	raw := eng.touched[:cap(eng.touched)]
	for i, tptr := range raw {
		if tptr != nil {
			t.Fatalf("touched[%d] still holds a trace after Consume", i)
		}
	}
}

type failingSink struct{ err error }

func (f failingSink) ConsumeSampled(context.Context, []*Span) error { return f.err }

func TestTouchedClearedOnSinkError(t *testing.T) {
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x15}, StatusCode: "ERROR"},
		{TraceID: TraceID{0x16}, StatusCode: "OK"},
	}); err == nil {
		t.Fatal("expected sink error")
	}
	raw := eng.touched[:cap(eng.touched)]
	for i, tptr := range raw {
		if tptr != nil {
			t.Fatalf("touched[%d] retained after sink error", i)
		}
	}
}

func TestShutdownSinkErrorDoesNotForceDropKeepWorthy(t *testing.T) {
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x17}, StatusCode: "ERROR"},
		{TraceID: TraceID{0x18}, StatusCode: "OK"},
	}); err == nil {
		t.Fatal("expected sink error from the ERROR trace")
	}
	if err := eng.Shutdown(context.Background()); err == nil {
		t.Fatal("expected Shutdown to surface the sink error")
	}
	if got := eng.NotSampled(); got != 0 {
		t.Fatalf("NotSampled() = %d, want 0 (keep-worthy trace must not be force-dropped)", got)
	}
	if counts := eng.BufferedSpanCounts([]TraceID{{0x17}}); counts[TraceID{0x17}] != 1 {
		t.Fatalf("keep-worthy trace was removed after sink error")
	}
}

func TestMaxTracesAdmitsNewErrorAfterDuePendingExpires(t *testing.T) {
	sink := &recordingSink{}
	base := time.Unix(0, 0)
	clk := base
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    1,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)
	eng.now = func() time.Time { return clk }

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x1f}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume OK: %v", err)
	}
	clk = base.Add(2 * time.Second)
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x20}, StatusCode: "ERROR"},
	}); err != nil {
		t.Fatalf("Consume ERROR: %v", err)
	}
	if got := eng.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d, want 0 (due Pending must expire before MaxTraces)", got)
	}
	if len(sink.got) != 1 {
		t.Fatalf("expected the new ERROR to be sampled, got %d batches", len(sink.got))
	}
}

func TestSinkErrorDoesNotExpireUnevaluatedKeepWorthy(t *testing.T) {
	base := time.Unix(0, 0)
	clk := base
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})
	eng.now = func() time.Time { return clk }

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x1d}, SpanID: SpanID{0x01}, StatusCode: "ERROR"},
		{TraceID: TraceID{0x1e}, SpanID: SpanID{0x02}, StatusCode: "ERROR"},
	}); err == nil {
		t.Fatal("expected sink error")
	}
	clk = base.Add(2 * time.Second)
	eng.expire()
	if got := eng.NotSampled(); got != 0 {
		t.Fatalf("NotSampled() = %d, want 0 (both ERROR traces must be held)", got)
	}
	counts := eng.BufferedSpanCounts([]TraceID{{0x1d}, {0x1e}})
	if counts[TraceID{0x1d}] != 1 {
		t.Fatalf("first ERROR trace was dropped")
	}
	if counts[TraceID{0x1e}] != 1 {
		t.Fatalf("second ERROR trace was dropped (never decided before expiry)")
	}
}

func TestExpiryDoesNotDropHeldSampledTrace(t *testing.T) {
	base := time.Unix(0, 0)
	clk := base
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})
	eng.now = func() time.Time { return clk }

	tid := TraceID{0x19}
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: tid, SpanID: SpanID{0x01}, StatusCode: "ERROR"},
	}); err == nil {
		t.Fatal("expected sink error")
	}
	clk = base.Add(2 * time.Second)
	eng.expire()
	if got := eng.NotSampled(); got != 0 {
		t.Fatalf("NotSampled() = %d, want 0 (held sampled trace must survive expiry)", got)
	}
	if counts := eng.BufferedSpanCounts([]TraceID{tid}); counts[tid] != 1 {
		t.Fatalf("held sampled trace was expired")
	}
}

func TestConsumeRetryAtSpanCapStillRedecides(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait:     time.Hour,
		MaxTraces:        100,
		MaxSpansPerTrace: 1,
		Policies:         []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})

	tid := TraceID{0x1b}
	span := &Span{TraceID: tid, SpanID: SpanID{0x01}, StatusCode: "ERROR"}
	if err := eng.Consume(context.Background(), []*Span{span}); err == nil {
		t.Fatal("expected sink error")
	}
	eng.sink = sink
	if err := eng.Consume(context.Background(), []*Span{span}); err != nil {
		t.Fatalf("at-cap retry Consume: %v", err)
	}
	if len(sink.got) != 1 || len(sink.got[0]) != 1 {
		t.Fatalf("at-cap retry must re-decide: got %d batches", len(sink.got))
	}
}

func TestShutdownCancelledCtxStillForwardsSampled(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x1c}, SpanID: SpanID{0x01}, StatusCode: "ERROR"},
	}); err == nil {
		t.Fatal("expected sink error")
	}
	eng.sink = sink
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := eng.Shutdown(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Shutdown: %v", err)
	}
	if len(sink.got) != 1 {
		t.Fatalf("cancelled Shutdown must still forward keep-worthy traces, got %d batches", len(sink.got))
	}
}

func TestConsumeRetryDoesNotDuplicateSpans(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    100,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})

	tid := TraceID{0x1a}
	span := &Span{TraceID: tid, SpanID: SpanID{0x01}, StatusCode: "ERROR"}
	if err := eng.Consume(context.Background(), []*Span{span}); err == nil {
		t.Fatal("expected sink error")
	}
	eng.sink = sink
	if err := eng.Consume(context.Background(), []*Span{span}); err != nil {
		t.Fatalf("retry Consume: %v", err)
	}
	if len(sink.got) != 1 {
		t.Fatalf("expected 1 forwarded batch, got %d", len(sink.got))
	}
	if got := len(sink.got[0]); got != 1 {
		t.Fatalf("forwarded %d spans, want 1 (retry must not duplicate)", got)
	}
}

func TestMaxTracesEvictsOldestPendingNotNewest(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    2,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x01}, StatusCode: "OK"},
		{TraceID: TraceID{0x02}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("fill buffer: %v", err)
	}
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x03}, StatusCode: "ERROR"},
	}); err != nil {
		t.Fatalf("admit new trace at cap: %v", err)
	}
	if got := eng.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d, want 0", got)
	}
	if got := eng.NotSampled(); got != 1 {
		t.Fatalf("NotSampled() = %d, want 1 (oldest Pending evicted)", got)
	}
	if len(sink.got) != 1 {
		t.Fatalf("expected ERROR trace sampled, got %d batches", len(sink.got))
	}
	if sink.got[0][0].TraceID != (TraceID{0x03}) {
		t.Fatalf("sampled trace id = %x, want 03", sink.got[0][0].TraceID)
	}
	counts := eng.BufferedSpanCounts([]TraceID{{0x01}, {0x02}, {0x03}})
	if counts[TraceID{0x01}] != 0 {
		t.Fatalf("oldest trace should have been evicted")
	}
	if counts[TraceID{0x02}] != 1 || counts[TraceID{0x03}] != 0 {
		t.Fatalf("buffer counts = %v, want 02=1 and 03 forwarded", counts)
	}
}

func TestMaxTracesDropsIncomingWhenNothingPending(t *testing.T) {
	eng := NewEngine(Config{
		DecisionWait: time.Hour,
		MaxTraces:    1,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, failingSink{err: errSink})

	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x21}, StatusCode: "ERROR"},
	}); err == nil {
		t.Fatal("expected sink error")
	}
	if err := eng.Consume(context.Background(), []*Span{
		{TraceID: TraceID{0x22}, StatusCode: "OK"},
	}); err != nil {
		t.Fatalf("Consume new trace: %v", err)
	}
	if got := eng.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d, want 1 (held Sampled trace blocks eviction)", got)
	}
}

func TestConcurrentConsumeMaintainsBufferConsistency(t *testing.T) {
	sink := &recordingSink{}
	eng := NewEngine(Config{
		DecisionWait: time.Second,
		MaxTraces:    256,
		Policies:     []Policy{StatusCodePolicy{Keep: "ERROR"}},
	}, sink)

	const workers = 8
	const spansPerWorker = 200
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			for i := 0; i < spansPerWorker; i++ {
				var tid TraceID
				tid[0] = byte(w)
				tid[1] = byte(i >> 8)
				tid[2] = byte(i)
				if err := eng.Consume(context.Background(), []*Span{
					{TraceID: tid, SpanID: SpanID{byte(i)}, StatusCode: "OK"},
				}); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}
	for w := 0; w < workers; w++ {
		if err := <-errCh; err != nil {
			t.Fatalf("worker Consume: %v", err)
		}
	}
	orderLen, traceLen := eng.bufferStateForTest()
	if orderLen != traceLen {
		t.Fatalf("order.Len()=%d, trace count=%d, want equal under concurrency", orderLen, traceLen)
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
