package bench

import (
	"context"
	"runtime"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor"
)

// These benchmarks are the head-to-head the isolation benches could not be:
// both processors see the same generated ptrace.Traces under the same keep
// policy (status_code=ERROR, latency≥200ms). The root module stays free of
// Collector/contrib — that is why this lives in its own module.
//
// Run:
//
//	make bench-h2h
//	go test -C bench -run '^$' -bench . -benchmem -benchtime=20000x
//
// Memory figures stabilize at large N; 20 000 is the documented comparison
// size (pdata copies make 200 000 heavier than the isolation engine bench).

func BenchmarkConsume(b *testing.B) {
	w := defaultWorkload(4096)
	gen := genBatches(w)
	cfg := procCfg{
		decisionWait: 30 * time.Second, // long enough that nothing expires mid-bench
		maxTraces:    10_000,           // eviction bounds in-flight traces
	}
	ctx := context.Background()

	for _, name := range []string{"astriena", "stock"} {
		b.Run(name, func(b *testing.B) {
			sink := &consumertest.TracesSink{}
			p, err := startNamed(name, sink, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer shutdown(p)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				td := gen.batches[i%len(gen.batches)]
				rewriteTraceID(td, uint64(i))
				if err := p.ConsumeTraces(ctx, td); err != nil {
					b.Fatalf("ConsumeTraces: %v", err)
				}
			}
			b.StopTimer()
			if name == "stock" {
				drainStock()
			}
		})
	}
}

// BenchmarkBufferBytes holds N pending traces in each processor (keepFrac=0,
// DecisionWait long enough that nothing expires during the fill) and attributes
// the live-heap delta per trace. The generated pdata is allocated before the
// first heap sample, so it cancels out of the delta — what remains is what
// each processor copied and indexed. The ratio of the two heapB/trace figures
// is the "fraction of the memory of the stock processor" claim.
func BenchmarkBufferBytes(b *testing.B) {
	cfg := procCfg{
		decisionWait: 30 * time.Second,
		maxTraces:    0, // set per run to b.N + headroom
	}
	ctx := context.Background()

	for _, name := range []string{"astriena", "stock"} {
		b.Run(name, func(b *testing.B) {
			w := defaultWorkload(b.N)
			w.keepFrac = 0
			gen := genBatches(w)
			cfg.maxTraces = b.N + 1
			if cfg.maxTraces < 2 {
				cfg.maxTraces = 2
			}

			sink := &consumertest.TracesSink{}
			p, err := startNamed(name, sink, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer shutdown(p)

			runtime.GC()
			var m0 runtime.MemStats
			runtime.ReadMemStats(&m0)

			b.ResetTimer()
			for _, td := range gen.batches {
				if err := p.ConsumeTraces(ctx, td); err != nil {
					b.Fatalf("ConsumeTraces: %v", err)
				}
			}
			if name == "stock" {
				drainStock()
			}
			b.StopTimer()

			runtime.GC()
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)

			if b.N > 0 {
				b.ReportMetric(float64(m1.HeapInuse-m0.HeapInuse)/float64(b.N), "heapB/trace")
			}
			runtime.KeepAlive(p)
			runtime.KeepAlive(gen)
		})
	}
}

func startNamed(name string, sink *consumertest.TracesSink, cfg procCfg) (processor.Traces, error) {
	switch name {
	case "astriena":
		return newAstriena(sink, cfg)
	case "stock":
		return newStock(sink, cfg)
	default:
		panic("unknown processor " + name)
	}
}
