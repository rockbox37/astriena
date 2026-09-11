package bench

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// workload is the pdata port of internal/sampling.genBatches: 4 spans/trace,
// 6 attributes/span, and keepFrac of traces carrying an ERROR or a >200 ms
// span. The same mix feeds both processors so the ratio is apples-to-apples.
type workload struct {
	traces        int
	spansPerTrace int
	attrsPerSpan  int
	keepFrac      float64
}

func defaultWorkload(traces int) workload {
	return workload{
		traces:        traces,
		spansPerTrace: 4,
		attrsPerSpan:  6,
		keepFrac:      0.20,
	}
}

// generated is one synthetic batch plus the keep-worthiness the generator
// planted, so tests can assert both processors kept the same traces.
type generated struct {
	batches []ptrace.Traces
	keep    []bool
}

func genBatches(w workload) generated {
	rng := rand.New(rand.NewSource(1))
	names := []string{"GET /", "GET /checkout", "POST /api/orders", "GET /healthz", "GET /assets"}
	out := generated{
		batches: make([]ptrace.Traces, 0, w.traces),
		keep:    make([]bool, 0, w.traces),
	}
	base := pcommon.Timestamp(1_000_000_000) // 1s, so durations stay in-range
	for i := 0; i < w.traces; i++ {
		var tid [16]byte
		binary.BigEndian.PutUint64(tid[:8], uint64(i))
		binary.BigEndian.PutUint64(tid[8:], uint64(i)*0x9e3779b97f4a7c15)

		keep := rng.Float64() < w.keepFrac
		hot := 0
		if w.spansPerTrace > 0 {
			hot = rng.Intn(w.spansPerTrace)
		}

		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "bench")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("astriena.bench")

		for j := 0; j < w.spansPerTrace; j++ {
			var sid [8]byte
			binary.BigEndian.PutUint64(sid[:], uint64(i)<<8|uint64(j))

			status := ptrace.StatusCodeOk
			dur := 5*time.Millisecond + time.Duration(rng.Int63n(int64(30*time.Millisecond)))
			if keep && j == hot {
				if rng.Intn(2) == 0 {
					status = ptrace.StatusCodeError
				} else {
					dur = 400*time.Millisecond + time.Duration(rng.Int63n(int64(200*time.Millisecond)))
				}
			}

			sp := ss.Spans().AppendEmpty()
			sp.SetTraceID(pcommon.TraceID(tid))
			sp.SetSpanID(pcommon.SpanID(sid))
			sp.SetName(names[rng.Intn(len(names))])
			sp.SetKind(ptrace.SpanKindServer)
			sp.Status().SetCode(status)
			sp.SetStartTimestamp(base)
			sp.SetEndTimestamp(base + pcommon.Timestamp(dur))
			for k := 0; k < w.attrsPerSpan; k++ {
				sp.Attributes().PutStr("attr."+strconv.Itoa(k), "v-"+strconv.Itoa(rng.Intn(1<<20)))
			}
		}
		out.batches = append(out.batches, td)
		out.keep = append(out.keep, keep)
	}
	return out
}

func cloneTraces(src ptrace.Traces) ptrace.Traces {
	dst := ptrace.NewTraces()
	src.CopyTo(dst)
	return dst
}

func traceIDOf(td ptrace.Traces) pcommon.TraceID {
	return td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID()
}

// rewriteTraceID stamps every span in td with a unique id derived from n so
// a looping consume bench does not append to the same in-flight traces.
func rewriteTraceID(td ptrace.Traces, n uint64) {
	var tid [16]byte
	binary.BigEndian.PutUint64(tid[:8], n)
	binary.BigEndian.PutUint64(tid[8:], n*0x9e3779b97f4a7c15)
	id := pcommon.TraceID(tid)
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				spans.At(k).SetTraceID(id)
			}
		}
	}
}

func sinkTraceIDs(all []ptrace.Traces) map[pcommon.TraceID]struct{} {
	ids := make(map[pcommon.TraceID]struct{})
	for _, td := range all {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					ids[spans.At(k).TraceID()] = struct{}{}
				}
			}
		}
	}
	return ids
}
