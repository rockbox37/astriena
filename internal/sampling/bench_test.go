package sampling

import (
	"context"
	"encoding/binary"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// This file benchmarks the pure engine in isolation — no Collector, no OTLP —
// which is what architecture.md calls for ("benchmark the engine in isolation";
// "Benchmark this package against the stock tail_sampling processor before
// optimizing"). It measures two dimensions:
//
//   - buffer memory per in-flight trace — the engine's own bookkeeping cost
//     (BenchmarkEngineBufferBytes). Engine-side only; the adapter-level
//     comparison against stock lives in bench/ and is recorded in
//     docs/benchmarks.md;
//   - ingest throughput and allocations, plus the realized keep ratio — the
//     "~80% ingestion reduction" claim (BenchmarkEngineConsume).
//
// These establish Astriena's own baseline. The head-to-head against the stock
// tail_sampling processor lives in the separate bench/ module (it needs the
// contrib processor and pdata) — see docs/benchmarks.md.

// blackholeSink discards forwarded spans so a sampled decision retains nothing —
// the buffer-memory measurement then reflects only what the engine itself holds.
type blackholeSink struct{ n int64 }

func (s *blackholeSink) ConsumeSampled(_ context.Context, _ []*Span) error {
	s.n++
	return nil
}

// workload describes a synthetic trace mix. Defaults model the target case: a
// flood of short, healthy traces the engine should drop, with a small fraction
// of errors and latency spikes it must keep.
type workload struct {
	traces        int
	spansPerTrace int
	attrsPerSpan  int
	keepFrac      float64 // fraction of traces carrying a kept (ERROR or slow) span
}

func defaultWorkload(traces int) workload {
	return workload{
		traces:        traces,
		spansPerTrace: 4,
		attrsPerSpan:  6,
		keepFrac:      0.20, // 80% healthy → dropped, matching the README claim
	}
}

// genBatches builds one span batch per trace (the shape the adapter hands the
// engine), each with a distinct trace id. A deterministic PRNG keeps runs
// comparable. Spans carry no Span.Raw payload: this isolates the engine's own
// domain footprint from the opaque exporter payload the adapter owns.
func genBatches(w workload) [][]*Span {
	rng := rand.New(rand.NewSource(1))
	names := []string{"GET /", "GET /checkout", "POST /api/orders", "GET /healthz", "GET /assets"}
	batches := make([][]*Span, 0, w.traces)
	for i := 0; i < w.traces; i++ {
		var tid TraceID
		binary.BigEndian.PutUint64(tid[:8], uint64(i))
		binary.BigEndian.PutUint64(tid[8:], uint64(i)*0x9e3779b97f4a7c15)

		keep := rng.Float64() < w.keepFrac
		// Which span in the trace carries the keep-worthy signal, if any.
		hot := rng.Intn(w.spansPerTrace)

		spans := make([]*Span, 0, w.spansPerTrace)
		for j := 0; j < w.spansPerTrace; j++ {
			var sid SpanID
			binary.BigEndian.PutUint64(sid[:], uint64(i)<<8|uint64(j))

			status, dur := "OK", int64(5*time.Millisecond)+rng.Int63n(int64(30*time.Millisecond))
			if keep && j == hot {
				if rng.Intn(2) == 0 {
					status = "ERROR"
				} else {
					dur = int64(400*time.Millisecond) + rng.Int63n(int64(200*time.Millisecond))
				}
			}

			attrs := make(map[string]string, w.attrsPerSpan)
			for k := 0; k < w.attrsPerSpan; k++ {
				attrs["attr."+strconv.Itoa(k)] = "v-" + strconv.Itoa(rng.Intn(1<<20))
			}

			spans = append(spans, &Span{
				TraceID:    tid,
				SpanID:     sid,
				Name:       names[rng.Intn(len(names))],
				Kind:       "SERVER",
				StatusCode: status,
				DurationNS: dur,
				Attributes: attrs,
			})
		}
		batches = append(batches, spans)
	}
	return batches
}

func keepPolicies() []Policy {
	return []Policy{
		StatusCodePolicy{Keep: "ERROR"},
		LatencyPolicy{ThresholdNS: int64(200 * time.Millisecond)},
	}
}

// BenchmarkEngineConsume measures steady-state ingest: b.N trace batches flow
// through Consume, each trace resolved within the iteration (kept traces decided
// by policy; healthy traces expired by advancing an injected clock past
// DecisionWait), so the buffer stays bounded instead of growing across the run.
// Reports the realized keep ratio alongside ns/op, B/op, and allocs/op.
func BenchmarkEngineConsume(b *testing.B) {
	w := defaultWorkload(4096)
	batches := genBatches(w)

	sink := &blackholeSink{}
	const decisionWait = time.Second
	eng := NewEngine(Config{
		DecisionWait: decisionWait,
		MaxTraces:    0, // unlimited: the clock-driven expiry below bounds the buffer
		Policies:     keepPolicies(),
	}, sink)

	base := time.Unix(0, 0)
	clk := base
	eng.now = func() time.Time { return clk }

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := eng.Consume(ctx, batches[i%len(batches)]); err != nil {
			b.Fatalf("Consume: %v", err)
		}
		// Advance past DecisionWait so the healthy traces buffered this iteration
		// are expired on the next Consume's inline expiry rather than accumulating.
		clk = clk.Add(decisionWait + time.Nanosecond)
	}
	b.StopTimer()

	kept, dropped := sink.n, eng.NotSampled()
	if total := kept + dropped; total > 0 {
		b.ReportMetric(float64(dropped)/float64(total)*100, "%dropped")
	}
}

// BenchmarkEngineBufferBytes measures the engine's steady-state memory cost per
// buffered in-flight trace — the isolation figure recorded in
// docs/benchmarks.md.
// b.N traces are held Pending (DecisionWait=0 disables expiry and no policy
// matches), and the live-heap delta is attributed per trace. Run with a large
// -benchtime (e.g. -benchtime=200000x) so the heap sample is stable.
func BenchmarkEngineBufferBytes(b *testing.B) {
	w := defaultWorkload(b.N)
	w.keepFrac = 0 // every trace stays Pending, so all b.N remain buffered
	batches := genBatches(w)

	sink := &blackholeSink{}
	eng := NewEngine(Config{
		DecisionWait: 0, // expiry disabled: traces persist so we can weigh them
		MaxTraces:    0, // unlimited
		Policies:     keepPolicies(),
	}, sink)
	ctx := context.Background()

	// Exclude the workload's own allocation from the measurement: sample the heap
	// after generating input, then again once every trace is buffered.
	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	// Fill one batch at a time. This is the shape that previously ran O(n^2) —
	// Consume re-scanned the whole buffer on every batch — and hung at large N;
	// with per-batch touched-only evaluation and expiry disabled here, it is O(n).
	b.ResetTimer()
	for _, batch := range batches {
		if err := eng.Consume(ctx, batch); err != nil {
			b.Fatalf("Consume: %v", err)
		}
	}
	b.StopTimer()

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	if b.N > 0 {
		b.ReportMetric(float64(m1.HeapInuse-m0.HeapInuse)/float64(b.N), "heapB/trace")
	}
	// Keep the engine (and its buffer) alive across the heap sample.
	runtime.KeepAlive(eng)
}
