package bench

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
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
//	make bench-h2h-concurrent
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

// BenchmarkConcurrentConsume exercises the same ingest path as BenchmarkConsume
// with N worker goroutines calling ConsumeTraces on one shared processor — the
// load shape lock striping (#8/#16) was built for. GOMAXPROCS is set to N for
// each sub-benchmark; worker counts are 1, 4, 8, and runtime.NumCPU() (deduped).
func BenchmarkConcurrentConsume(b *testing.B) {
	w := defaultWorkload(4096)
	gen := genBatches(w)
	cfg := procCfg{
		decisionWait: 30 * time.Second,
		maxTraces:    10_000,
	}
	ctx := context.Background()

	for _, workers := range concurrencyLevels() {
		workers := workers
		for _, name := range []string{"astriena", "stock"} {
			b.Run(fmt.Sprintf("workers=%d/%s", workers, name), func(b *testing.B) {
				prevProcs := runtime.GOMAXPROCS(workers)

				sink := &consumertest.TracesSink{}
				p, err := startNamed(name, sink, cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					shutdown(p)
					runtime.GOMAXPROCS(prevProcs)
				}()

				var seq atomic.Uint64
				var consumeErr atomic.Value

				b.ReportAllocs()
				b.SetParallelism(workers)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if consumeErr.Load() != nil {
							return
						}
						i := seq.Add(1)
						td := cloneTraces(gen.batches[i%uint64(len(gen.batches))])
						rewriteTraceID(td, i)
						if err := p.ConsumeTraces(ctx, td); err != nil {
							consumeErr.Store(err)
							return
						}
					}
				})
				if err := consumeErr.Load(); err != nil {
					b.Fatalf("ConsumeTraces: %v", err)
				}
				b.StopTimer()
				if name == "stock" {
					drainStock()
				}
			})
		}
	}
}

// concurrencyLevels returns 1, 4, 8, and runtime.NumCPU(), deduplicated and
// sorted, for the concurrent ingest sub-benchmarks.
func concurrencyLevels() []int {
	n := runtime.NumCPU()
	seen := make(map[int]struct{}, 4)
	out := make([]int, 0, 4)
	for _, g := range []int{1, 4, 8, n} {
		if g < 1 {
			continue
		}
		if _, ok := seen[g]; ok {
			continue
		}
		seen[g] = struct{}{}
		out = append(out, g)
	}
	return out
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
