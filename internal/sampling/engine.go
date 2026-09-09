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
// it (see expireLocked). Expiry runs inline on every Consume (cheap under load)
// and, so buffered traces are still flushed when ingest goes quiet, on a
// background ticker started by Start. This default-drop is what realizes the
// ingestion reduction: redundant traces leave the buffer instead of piling up
// until MaxTraces evicts new ones.
//
// MaxTraces is enforced as a hard memory bound (new traces are dropped once the
// cap is reached). TODO(core): the production hot path still needs a sharded /
// lock-striped buffer so a single mutex is not the bottleneck under load (this is
// where the memory/throughput wedge is won or lost), a time-ordered structure so
// expiry does not scan every buffered trace, and smarter eviction than
// drop-newest (e.g. evict the oldest still-Pending trace). Benchmark this package
// against the stock tail_sampling processor before optimizing — see
// docs/architecture.md.
type Engine struct {
	cfg  Config
	sink Sink
	// now returns the current time; injectable so expiry is testable without
	// sleeping. Defaults to time.Now.
	now func() time.Time

	mu         sync.Mutex
	traces     map[TraceID]*Trace
	dropped    int64
	notSampled int64

	// stop/done coordinate the background expiry ticker (see Start/Shutdown).
	stop chan struct{}
	done chan struct{}
}

// NewEngine constructs an Engine that forwards sampled traces to sink. Call
// Start to run background expiry and Shutdown to stop it and drain the buffer.
func NewEngine(cfg Config, sink Sink) *Engine {
	return &Engine{
		cfg:    cfg,
		sink:   sink,
		now:    time.Now,
		traces: make(map[TraceID]*Trace),
	}
}

// Start launches the background ticker that forces decisions on traces whose
// DecisionWait has elapsed even when no new spans arrive. It is a no-op when
// DecisionWait is unset (time-based expiry disabled) or already running.
func (e *Engine) Start() {
	if e.cfg.DecisionWait <= 0 || e.stop != nil {
		return
	}
	interval := e.cfg.DecisionWait / 2
	if interval <= 0 {
		interval = e.cfg.DecisionWait
	}
	e.stop = make(chan struct{})
	e.done = make(chan struct{})
	go func() {
		defer close(e.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-e.stop:
				return
			case <-t.C:
				// TODO(core): a background flush error is currently swallowed;
				// surface it once the engine has a logger/metrics handle.
				_ = e.sweep(context.Background())
			}
		}
	}()
}

// Shutdown stops the background ticker and forces a terminal decision on every
// remaining buffered trace: any still Pending is dropped (the default). It
// returns ctx.Err() if ctx is cancelled while waiting for the ticker to stop.
func (e *Engine) Shutdown(ctx context.Context) error {
	if e.stop != nil {
		close(e.stop)
		select {
		case <-e.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		e.stop = nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.evaluateLocked(ctx); err != nil {
		return err
	}
	e.expireLocked(e.now(), true)
	return nil
}

// Consume buffers a batch of spans and evaluates the traces they touch.
func (e *Engine) Consume(ctx context.Context, spans []*Span) error {
	e.mu.Lock()
	defer e.mu.Unlock()

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
			t = &Trace{ID: s.TraceID, Arrived: e.now()}
			e.traces[s.TraceID] = t
		}
		// Bound spans for a single trace: one runaway trace id must not grow
		// without limit even though the trace-count cap is not reached.
		if e.cfg.MaxSpansPerTrace > 0 && len(t.Spans) >= e.cfg.MaxSpansPerTrace {
			e.dropped++
			continue
		}
		t.Spans = append(t.Spans, s)
	}
	return e.sweepLocked(ctx)
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

// sweep runs one evaluate+expire pass under the lock. The background ticker and
// the deterministic tests drive expiry through it.
func (e *Engine) sweep(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sweepLocked(ctx)
}

func (e *Engine) sweepLocked(ctx context.Context) error {
	if err := e.evaluateLocked(ctx); err != nil {
		return err
	}
	e.expireLocked(e.now(), false)
	return nil
}

// expireLocked forces the default NotSampled decision on every still-Pending
// trace whose DecisionWait has elapsed, dropping it from the buffer. When force
// is set, every remaining Pending trace is dropped regardless of deadline (used
// by Shutdown to drain). With DecisionWait unset and force false, expiry is
// disabled and traces persist until a policy decides them or MaxTraces evicts.
func (e *Engine) expireLocked(now time.Time, force bool) {
	if e.cfg.DecisionWait <= 0 && !force {
		return
	}
	for id, t := range e.traces {
		if t.decision != DecisionPending {
			continue
		}
		if force || !now.Before(t.Arrived.Add(e.cfg.DecisionWait)) {
			t.decision = DecisionNotSampled
			e.notSampled++
			delete(e.traces, id)
		}
	}
}

func (e *Engine) evaluateLocked(ctx context.Context) error {
	for id, t := range e.traces {
		if t.decision != DecisionPending {
			continue
		}
		for _, p := range e.cfg.Policies {
			d := p.Evaluate(t)
			if d == DecisionPending {
				continue
			}
			t.decision = d
			if d == DecisionSampled {
				if err := e.sink.ConsumeSampled(ctx, t.Spans); err != nil {
					return err
				}
			}
			delete(e.traces, id)
			break
		}
	}
	return nil
}
