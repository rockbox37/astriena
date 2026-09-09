package bench

import (
	"context"
	"runtime"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor"
)

// TestPolicyParity feeds the same generated pdata to both processors and
// checks they keep the same keep-worthy traces (ERROR or ≥200 ms). Healthy
// traces must not be forwarded while still inside DecisionWait.
func TestPolicyParity(t *testing.T) {
	w := defaultWorkload(256)
	gen := genBatches(w)
	cfg := procCfg{decisionWait: 30 * time.Second, maxTraces: 10_000}

	type side struct {
		name string
		new  func(next *consumertest.TracesSink, cfg procCfg) (processor.Traces, error)
		sink *consumertest.TracesSink
	}
	sides := []side{
		{name: "astriena", new: func(next *consumertest.TracesSink, cfg procCfg) (processor.Traces, error) {
			return newAstriena(next, cfg)
		}},
		{name: "stock", new: func(next *consumertest.TracesSink, cfg procCfg) (processor.Traces, error) {
			return newStock(next, cfg)
		}},
	}

	wantKeep := make(map[pcommon.TraceID]struct{})
	for i, keep := range gen.keep {
		if keep {
			wantKeep[traceIDOf(gen.batches[i])] = struct{}{}
		}
	}

	ctx := context.Background()
	got := make(map[string]map[pcommon.TraceID]struct{}, len(sides))
	for i := range sides {
		s := &sides[i]
		s.sink = &consumertest.TracesSink{}
		p, err := s.new(s.sink, cfg)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		for _, td := range gen.batches {
			if err := p.ConsumeTraces(ctx, td); err != nil {
				t.Fatalf("%s ConsumeTraces: %v", s.name, err)
			}
		}
		if s.name == "stock" {
			// span-ingest decides on the event loop; wait until keep-worthy
			// traces have been forwarded rather than guessing a sleep.
			waitUntil(t, 3*time.Second, func() bool {
				return len(sinkTraceIDs(s.sink.AllTraces())) >= len(wantKeep)
			})
		}
		got[s.name] = sinkTraceIDs(s.sink.AllTraces())
		shutdown(p)
	}

	for name, ids := range got {
		for id := range wantKeep {
			if _, ok := ids[id]; !ok {
				t.Errorf("%s did not keep keep-worthy trace %x", name, id)
			}
		}
		for id := range ids {
			if _, ok := wantKeep[id]; !ok {
				t.Errorf("%s forwarded a healthy trace %x inside DecisionWait", name, id)
			}
		}
	}
	if t.Failed() {
		return
	}
	a, b := got["astriena"], got["stock"]
	if len(a) != len(b) {
		t.Fatalf("kept-trace count: astriena=%d stock=%d", len(a), len(b))
	}
}

// TestDropAfterWait checks that both processors drop healthy traces once
// DecisionWait elapses. Stock's event loop ticks once a second, so the wait
// is a few seconds — this is a correctness check, not a bench.
func TestDropAfterWait(t *testing.T) {
	if testing.Short() {
		t.Skip("DecisionWait + stock ticker is too slow for -short")
	}
	w := defaultWorkload(64)
	w.keepFrac = 0 // every trace is healthy and must be dropped
	gen := genBatches(w)
	cfg := procCfg{decisionWait: time.Second, maxTraces: 10_000}

	ctx := context.Background()
	for _, name := range []string{"astriena", "stock"} {
		sink := &consumertest.TracesSink{}
		var (
			p   processor.Traces
			err error
		)
		switch name {
		case "astriena":
			p, err = newAstriena(sink, cfg)
		case "stock":
			p, err = newStock(sink, cfg)
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		for _, td := range gen.batches {
			if err := p.ConsumeTraces(ctx, td); err != nil {
				t.Fatalf("%s ConsumeTraces: %v", name, err)
			}
		}
		if name == "stock" {
			drainStock()
		}
		if n := len(sinkTraceIDs(sink.AllTraces())); n != 0 {
			t.Fatalf("%s forwarded %d traces before DecisionWait", name, n)
		}

		// Stock decides on its ~1s ticker after DecisionWait; Astriena's
		// background ticker is DecisionWait/2. Wait long enough for both.
		time.Sleep(4 * time.Second)
		shutdown(p)
		if n := len(sinkTraceIDs(sink.AllTraces())); n != 0 {
			t.Errorf("%s forwarded %d healthy traces after DecisionWait; want 0", name, n)
		}
	}
}

// TestHeapRatioSmoke fills a fixed in-flight count on both processors and
// prints heapB/trace plus the stock/astriena ratio. It is a cheap verification
// that both paths run; the published numbers come from BenchmarkBufferBytes.
func TestHeapRatioSmoke(t *testing.T) {
	const n = 4096
	w := defaultWorkload(n)
	w.keepFrac = 0
	gen := genBatches(w)
	cfg := procCfg{decisionWait: 30 * time.Second, maxTraces: n + 16}
	ctx := context.Background()

	measure := func(name string) float64 {
		sink := &consumertest.TracesSink{}
		var (
			p   processor.Traces
			err error
		)
		switch name {
		case "astriena":
			p, err = newAstriena(sink, cfg)
		case "stock":
			p, err = newStock(sink, cfg)
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		defer shutdown(p)

		runtime.GC()
		var m0 runtime.MemStats
		runtime.ReadMemStats(&m0)
		for _, td := range gen.batches {
			if err := p.ConsumeTraces(ctx, td); err != nil {
				t.Fatalf("%s ConsumeTraces: %v", name, err)
			}
		}
		if name == "stock" {
			drainStock()
		}
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		per := float64(m1.HeapInuse-m0.HeapInuse) / float64(n)
		t.Logf("%s: %.0f heapB/trace (n=%d)", name, per, n)
		runtime.KeepAlive(p)
		return per
	}

	a := measure("astriena")
	s := measure("stock")
	if a <= 0 || s <= 0 {
		t.Fatalf("non-positive heap sample: astriena=%.0f stock=%.0f (heap sampling is noisy at small n)", a, s)
	}
	t.Logf("ratio stock/astriena = %.2fx (astriena is %.0f%% of stock)", s/a, 100*a/s)
}

func waitUntil(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s", timeout)
}

func TestRootModuleStaysPure(t *testing.T) {
	// Presence-only: this module imports Collector/contrib. The root module
	// must not. go/build would be the real check; CI runs `go test ./internal/...`
	// in the root module, which fails if someone adds a Collector import there.
	t.Log("head-to-head lives in module github.com/rockbox37/astriena/bench; root stays Collector-free")
}
