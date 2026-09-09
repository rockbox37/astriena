// Package sampling implements Astriena's tail-sampling engine.
//
// It is deliberately framework-free: it has no dependency on the OpenTelemetry
// Collector or on OTLP wire types. The Collector processor adapter in
// components/astrienasampler translates pdata into these domain types and back.
//
// Keeping the engine pure is what lets Astriena later swap the surrounding
// framework — or reimplement this hot path in another language (e.g. Rust) —
// without rewriting the sampling logic. This package is also the memory wedge:
// its buffering efficiency is what lets Astriena claim "a fraction of the memory
// of the stock processor". See docs/architecture.md.
package sampling

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// TraceID and SpanID are opaque identifiers copied out of the ingest layer.
type TraceID [16]byte
type SpanID [8]byte

// Span is the minimal representation the engine needs to make keep/drop
// decisions. The adapter populates it from OTLP; fields are added as policies
// require them.
type Span struct {
	TraceID    TraceID
	SpanID     SpanID
	Name       string
	Kind       string
	StatusCode string // "", "OK", "ERROR"
	DurationNS int64
	Attributes map[string]string

	// Raw carries the original span payload the exporter will write if the
	// trace is sampled. It is opaque to the engine.
	Raw any
}

// Decision is the outcome of evaluating a trace.
type Decision int

const (
	DecisionPending Decision = iota
	DecisionSampled
	DecisionNotSampled
)

// Trace is the set of spans buffered for one trace id, plus bookkeeping.
type Trace struct {
	ID      TraceID
	Spans   []*Span
	Arrived time.Time

	decision Decision

	// elem is this trace's node in the engine's arrival-ordered list, held so it
	// can be unlinked in O(1) when the trace is decided or expired.
	elem *list.Element
	// touchedGen marks the last Consume generation that added a span to this
	// trace, so a single batch evaluates each touched trace exactly once.
	touchedGen uint64
}

// Policy decides whether a (possibly partial) trace should be kept. Policies
// must be side-effect free and fast: Evaluate is called on the hot path.
type Policy interface {
	// Name identifies the policy in metrics and logs.
	Name() string
	// Evaluate returns Sampled/NotSampled to decide now, or Pending to defer
	// until more spans arrive or the decision wait elapses.
	Evaluate(*Trace) Decision
}

// Sink receives the spans of traces the engine decides to keep.
type Sink interface {
	ConsumeSampled(ctx context.Context, spans []*Span) error
}

// Config controls the engine's buffering behavior.
type Config struct {
	// DecisionWait is how long to buffer a trace before forcing a decision.
	DecisionWait time.Duration
	// MaxTraces caps the number of in-flight traces (memory safeguard).
	MaxTraces int
	// MaxSpansPerTrace caps how many spans the engine buffers for a single
	// trace — a safeguard against one runaway trace id growing without bound
	// across batches. <=0 disables. This bounds the engine's per-trace domain
	// spans; the opaque payload each span carries (Span.Raw) is the adapter's
	// concern (see toEngineSpans).
	MaxSpansPerTrace int
	// Policies are evaluated in order; the first non-Pending decision wins.
	Policies []Policy
}

// Engine buffers spans by trace id, applies policies, and forwards the spans of
// sampled traces to the Sink.
//
// A trace no policy chooses to keep stays Pending only until DecisionWait
// elapses; then the engine forces the default decision — NotSampled — and drops
// it (see expireLocked). Expiry runs inline on every Consume and, so buffered
// traces are still flushed when ingest goes quiet, on a background ticker started
// by Start. This default-drop is what realizes the ingestion reduction: redundant
// traces leave the buffer instead of piling up until MaxTraces evicts new ones.
//
// Two structures keep the hot path off O(n) per batch:
//   - traces indexes buffered traces by id for O(1) lookup on ingest.
//   - order is a FIFO list of the same traces in arrival order. Because every
//     trace shares one DecisionWait, arrival order is deadline order: expiry pops
//     the front while it is due and stops at the first trace that is not — O(k)
//     in the number actually expiring, not O(n) in the buffer. A trace decided
//     early is unlinked in O(1) via the *list.Element it holds.
//
// Consume also re-evaluates only the traces the current batch touched, not the
// whole buffer: policies are pure functions of a trace's spans, so a trace that
// gained no span cannot change its decision. Together these make a stream of
// batches O(total spans) rather than O(n^2) in buffer size.
//
// MaxTraces is enforced as a hard memory bound (new traces are dropped once the
// cap is reached). TODO(core): what remains for the production hot path is a
// sharded / lock-striped buffer so a single mutex is not the bottleneck under
// load (this is where the throughput wedge is won or lost) and smarter eviction
// than drop-newest at the cap (e.g. evict the oldest still-Pending trace, now
// cheap to find at the front of order). Benchmark against the stock tail_sampling
// processor before optimizing further — see docs/benchmarks.md.
type Engine struct {
	cfg  Config
	sink Sink
	// now returns the current time; injectable so expiry is testable without
	// sleeping. Defaults to time.Now.
	now func() time.Time

	mu         sync.Mutex
	traces     map[TraceID]*Trace
	order      *list.List // *Trace in arrival order; front is oldest (see above)
	dropped    int64
	notSampled int64

	// gen counts Consume calls; touched collects the traces a single Consume
	// added spans to, so evaluation touches each such trace once (see Consume).
	gen     uint64
	touched []*Trace

	// stop/done coordinate the background expiry ticker (see Start/Shutdown).
	// stopped is set the first time stop is closed so a second Shutdown cannot
	// panic on a closed channel; stop is cleared only after the ticker exits.
	stop    chan struct{}
	done    chan struct{}
	stopped bool
}

// NewEngine constructs an Engine that forwards sampled traces to sink. Call
// Start to run background expiry and Shutdown to stop it and drain the buffer.
func NewEngine(cfg Config, sink Sink) *Engine {
	return &Engine{
		cfg:    cfg,
		sink:   sink,
		now:    time.Now,
		traces: make(map[TraceID]*Trace),
		order:  list.New(),
	}
}

// Start launches the background ticker that forces decisions on traces whose
// DecisionWait has elapsed even when no new spans arrive. It is a no-op when
// DecisionWait is unset (time-based expiry disabled) or already running.
func (e *Engine) Start() {
	e.mu.Lock()
	if e.cfg.DecisionWait <= 0 || e.stop != nil {
		e.mu.Unlock()
		return
	}
	interval := e.cfg.DecisionWait / 2
	if interval <= 0 {
		interval = e.cfg.DecisionWait
	}
	e.stopped = false
	e.stop = make(chan struct{})
	e.done = make(chan struct{})
	e.mu.Unlock()
	go func() {
		defer close(e.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-e.stop:
				return
			case <-t.C:
				// Expiry only: without new spans no trace can newly match a
				// policy, so the ticker never evaluates (and never touches the
				// sink) — it just drops traces whose DecisionWait has elapsed.
				e.expire()
			}
		}
	}()
}

// Shutdown stops the background ticker and forces a terminal decision on every
// remaining buffered trace: any still Pending is dropped (the default). It
// returns ctx.Err() if ctx is cancelled while waiting for the ticker to stop.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	stop, done := e.stop, e.done
	if stop != nil && !e.stopped {
		e.stopped = true
		close(stop)
	}
	e.mu.Unlock()

	var waitErr error
	if stop != nil {
		select {
		case <-done:
			e.mu.Lock()
			e.stop = nil
			e.mu.Unlock()
		case <-ctx.Done():
			// Best-effort drain below even if the ticker has not acknowledged
			// stop. Leave stop set so Start stays a no-op and a second
			// Shutdown cannot close the channel again.
			waitErr = ctx.Err()
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// Drain: give every remaining trace a final policy pass (a full scan, but
	// only once, at shutdown) so a Sampled one is still forwarded, then force the
	// default NotSampled decision on whatever stays Pending. Skip the force-drop
	// if evaluation failed so a keep-worthy trace is left for a retry Shutdown.
	// Drain with Background so a cancelled Shutdown ctx cannot fail every
	// ConsumeSampled and leave keep-worthy traces unforwarded.
	evalErr := e.evaluateAllLocked(context.Background())
	if evalErr == nil {
		e.expireLocked(e.now(), true)
	}
	if evalErr != nil {
		return evalErr
	}
	return waitErr
}

// Consume buffers a batch of spans and evaluates the traces they touch.
func (e *Engine) Consume(ctx context.Context, spans []*Span) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Snapshot the clock once so DecisionWait does not count sink I/O that
	// holds this lock: Arrived and expiry must agree on "now" for this batch.
	now := e.now()

	// Free cap slots taken by due Pending traces this batch cannot refresh,
	// so MaxTraces does not drop a new keep-worthy id that would have fit.
	// Incoming ids are skipped: a due trace may be about to receive a late ERROR.
	incoming := make(map[TraceID]struct{}, len(spans))
	for _, s := range spans {
		incoming[s.TraceID] = struct{}{}
	}
	e.expireLockedSkip(now, false, incoming)

	// Collect the traces this batch adds spans to. Only these can change decision
	// (policies are pure functions of a trace's spans), so only these are
	// evaluated below — the buffer at large is left untouched.
	e.gen++
	e.touched = e.touched[:0]

	for _, s := range spans {
		t, ok := e.traces[s.TraceID]
		if !ok {
			// MaxTraces is a hard safeguard against unbounded buffer growth: once
			// the in-flight cap is reached, drop spans for new traces rather than
			// grow without bound. Spans for traces already buffered still append.
			if e.cfg.MaxTraces > 0 && len(e.traces) >= e.cfg.MaxTraces {
				e.dropped++
				continue
			}
			t = &Trace{ID: s.TraceID, Arrived: now}
			e.traces[s.TraceID] = t
			// Link at the back: traces enter order oldest-first, which is also
			// deadline order since they share one DecisionWait (see expireLocked).
			t.elem = e.order.PushBack(t)
		}
		// Bound spans for a single trace: one runaway trace id must not grow
		// without limit even though the trace-count cap is not reached.
		if e.cfg.MaxSpansPerTrace > 0 && len(t.Spans) >= e.cfg.MaxSpansPerTrace {
			e.dropped++
			// An existing trace at the cap must still be re-decided (sink
			// retry) when the retried batch is entirely past the cap.
			if ok && t.touchedGen != e.gen {
				t.touchedGen = e.gen
				e.touched = append(e.touched, t)
			}
			continue
		}
		dup := false
		if s.SpanID != (SpanID{}) {
			for _, existing := range t.Spans {
				if existing.SpanID == s.SpanID {
					dup = true
					break
				}
			}
		}
		if !dup {
			t.Spans = append(t.Spans, s)
		}
		// A retried Consume must still re-decide (sink retry) even when every
		// span was already buffered.
		if t.touchedGen != e.gen {
			t.touchedGen = e.gen
			e.touched = append(e.touched, t)
		}
	}

	// Decide every touched trace before returning a sink error, so a later
	// keep-worthy trace is marked Sampled-held instead of left Pending for
	// DecisionWait to default-drop. Always expire after decide-all:
	// expireLocked skips DecisionSampled, so keep-worthy traces stay held.
	var first error
	for _, t := range e.touched {
		if err := e.decideLocked(ctx, t); err != nil && first == nil {
			first = err
		}
	}
	clear(e.touched)
	e.touched = e.touched[:0]
	e.expireLocked(now, false)
	return first
}

// Dropped returns the number of spans dropped by a memory safeguard — either
// the MaxTraces cap (a new trace at the trace-count limit) or the
// MaxSpansPerTrace cap (a span past a trace's per-trace limit). This counts
// memory-pressure drops only, not traces decided NotSampled by policy or by
// DecisionWait expiry — see NotSampled.
// TODO(core): surface this as a Collector metric; split the two reasons if they
// need to be distinguished.
func (e *Engine) Dropped() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// NotSampled returns the number of traces given the default NotSampled decision
// because DecisionWait elapsed with no policy choosing to keep them — the
// engine's normal ingestion-reduction path, distinct from the memory-safeguard
// drops counted by Dropped.
func (e *Engine) NotSampled() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.notSampled
}

// expire runs one expiry pass under the lock. The background ticker drives quiet
// buffered traces to a decision through it; it never evaluates policies or
// touches the sink, so it cannot fail.
func (e *Engine) expire() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expireLocked(e.now(), false)
}

// removeLocked deletes a decided/expired trace from both the id index and the
// arrival-ordered list (the latter in O(1) via the node the trace holds).
func (e *Engine) removeLocked(t *Trace) {
	delete(e.traces, t.ID)
	if t.elem != nil {
		e.order.Remove(t.elem)
		t.elem = nil
	}
}

// decideLocked applies the policies to one still-Pending trace. The first
// non-Pending policy wins: the trace takes that decision, is forwarded when
// Sampled, and is removed from the buffer. Returns any sink error.
func (e *Engine) decideLocked(ctx context.Context, t *Trace) error {
	if t.decision == DecisionSampled {
		// Prior sink failure: retry forward; do not default-drop this trace.
		if err := e.sink.ConsumeSampled(ctx, t.Spans); err != nil {
			return err
		}
		e.removeLocked(t)
		return nil
	}
	if t.decision != DecisionPending {
		return nil
	}
	for _, p := range e.cfg.Policies {
		d := p.Evaluate(t)
		if d == DecisionPending {
			continue
		}
		if d == DecisionSampled {
			if err := e.sink.ConsumeSampled(ctx, t.Spans); err != nil {
				t.decision = DecisionSampled
				return err
			}
		}
		t.decision = d
		e.removeLocked(t)
		return nil
	}
	return nil
}

// evaluateAllLocked runs decideLocked over every buffered trace. It is a full
// O(n) scan and is used only on Shutdown to drain; the hot path evaluates only
// the traces a batch touched (see Consume).
func (e *Engine) evaluateAllLocked(ctx context.Context) error {
	var first error
	for _, t := range e.traces {
		if err := e.decideLocked(ctx, t); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// expireLocked forces the default NotSampled decision on still-Pending traces
// whose DecisionWait has elapsed, dropping them from the buffer. Traces held at
// DecisionSampled after a sink failure are skipped — they must be retried, not
// default-dropped. It walks the arrival-ordered list from the front (oldest)
// and stops at the first Pending trace not yet due — because all traces share
// one DecisionWait, arrival order is deadline order among Pending traces. When
// force is set, every remaining Pending trace is dropped regardless of deadline
// (used by Shutdown to drain).
// With DecisionWait unset and force false, expiry is disabled and traces persist
// until a policy decides them or MaxTraces evicts.
//
// This relies on now() being monotonic non-decreasing across Consume calls (true
// for time.Now and for the forward-only clocks the tests inject); a clock that
// ran backwards could leave a due trace behind the stop point until the next
// pass.
func (e *Engine) expireLocked(now time.Time, force bool) {
	e.expireLockedSkip(now, force, nil)
}

func (e *Engine) expireLockedSkip(now time.Time, force bool, skip map[TraceID]struct{}) {
	if e.cfg.DecisionWait <= 0 && !force {
		return
	}
	for elem := e.order.Front(); elem != nil; {
		t := elem.Value.(*Trace)
		next := elem.Next()
		// Keep-worthy traces whose sink failed stay DecisionSampled in the
		// buffer; the ticker must not default-drop them.
		if t.decision != DecisionPending {
			elem = next
			continue
		}
		if !force && now.Before(t.Arrived.Add(e.cfg.DecisionWait)) {
			return
		}
		if !force && skip != nil {
			if _, keep := skip[t.ID]; keep {
				elem = next
				continue
			}
		}
		t.decision = DecisionNotSampled
		e.notSampled++
		e.removeLocked(t)
		elem = next
	}
}
