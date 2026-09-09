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
// MaxTraces is enforced as a hard memory bound (new traces are dropped once the
// cap is reached). TODO(core): the production hot path still needs a sharded /
// lock-striped buffer so a single mutex is not the bottleneck under load (this is
// where the memory/throughput wedge is won or lost), a time-ordered structure to
// force a decision once DecisionWait elapses, and smarter eviction than
// drop-newest (e.g. evict the oldest still-Pending trace). Benchmark this package
// against the stock tail_sampling processor before optimizing — see
// docs/architecture.md.
type Engine struct {
	cfg  Config
	sink Sink

	mu      sync.Mutex
	traces  map[TraceID]*Trace
	dropped int64
}

// NewEngine constructs an Engine that forwards sampled traces to sink.
func NewEngine(cfg Config, sink Sink) *Engine {
	return &Engine{
		cfg:    cfg,
		sink:   sink,
		traces: make(map[TraceID]*Trace),
	}
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
			t = &Trace{ID: s.TraceID, Arrived: time.Now()}
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
	// TODO(core): decouple decision from ingest via DecisionWait timers instead
	// of evaluating inline on every batch.
	return e.evaluateLocked(ctx)
}

// Dropped returns the number of spans dropped by a memory safeguard — either
// the MaxTraces cap (a new trace at the trace-count limit) or the
// MaxSpansPerTrace cap (a span past a trace's per-trace limit).
// TODO(core): surface this as a Collector metric; split the two reasons if they
// need to be distinguished.
func (e *Engine) Dropped() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
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
