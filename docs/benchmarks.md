# Benchmarks — proving the wedge

Astriena's pitch rests on two measurable claims:

- **"A fraction of the memory of the stock processor."**
- **"~80% ingestion reduction"** — dropping redundant healthy traces while keeping
  100% of errors and latency spikes.

This document records how those are measured and the numbers as of the last run.
[`architecture.md`](architecture.md) calls for exactly this — *"benchmark the
engine in isolation"* and *"benchmark this package against the stock
`tail_sampling` processor before optimizing"* — so these numbers are the baseline
that must exist **before** the hot-path optimizations in the `TODO(core)` note in
[`internal/sampling/engine.go`](../internal/sampling/engine.go) are attempted.

## Running them

```sh
make bench
# or, to vary the sample size (memory figures stabilize at large N):
go test ./internal/sampling/ -run '^$' -bench . -benchmem -benchtime=200000x
```

The benchmarks live in
[`internal/sampling/bench_test.go`](../internal/sampling/bench_test.go) and run
**offline** against the pure engine — no Collector, no OTLP. The synthetic
workload is a trace mix modeling the target case: 4 spans/trace, 6
attributes/span, and 20% of traces carrying an `ERROR` or a >200 ms span (the
keep-worthy signal); the other 80% are short, healthy traces the engine should
drop.

## Results (baseline)

Apple M4, `go test`, `-benchtime=200000x`. Reproduce with `make bench`; absolute
numbers are hardware-dependent — the ratios and the shape are the point.

| Benchmark | Metric | Value |
|---|---|---|
| `BenchmarkEngineConsume` | throughput | ~222 ns per 4-span trace |
| | allocations | 4 allocs/op, 136 B/op |
| | **realized drop rate** | **79.99 %** |
| `BenchmarkEngineBufferBytes` | **engine bookkeeping per buffered trace** | **~120–180 heap B/trace** (varies with N as the map grows/fragments) |

### What the drop rate proves

`BenchmarkEngineConsume` reports `%dropped` straight from the engine's own
counters (`NotSampled` vs forwarded). At a 20% keep fraction it drops **79.99%**
of traces — the "~80% ingestion reduction" claim, realized end-to-end through the
real policy + `DecisionWait` default-drop path, not asserted. Every kept trace is
one a policy chose (`status_code` = ERROR, or `latency` ≥ 200 ms); nothing
keep-worthy is dropped.

### What the memory figure measures (and what it deliberately excludes)

`BenchmarkEngineBufferBytes` holds `b.N` traces `Pending` (expiry disabled, no
policy match) and attributes the live-heap delta per trace. The figure is the
engine's **own bookkeeping**: the `map[TraceID]*Trace` entry, the `Trace` struct,
its arrival-list node (`*list.Element`), and the per-trace span-pointer slice.

It **excludes** the span payloads and attribute maps — those are allocated by the
workload generator *before* the heap is sampled, so they do not count. This is
deliberate: the span payload exists in any processor and, in the real adapter, is
dominated by the opaque `Span.Raw` pdata the exporter will write (see
`toEngineSpans`). The wedge Astriena controls is the per-trace bookkeeping
overhead, and that is what this isolates. The head-to-head below is what turns
this into a *ratio* against the stock processor.

## A finding this surfaced — and its fix: evaluate/expiry were O(n²)

Writing the memory benchmark exposed that `Engine.Consume` re-evaluated **every**
buffered trace on **every** batch (`evaluateLocked` ranged the whole `traces`
map, and `expireLocked` did too). Filling the buffer one batch at a time was
therefore O(n²) — a 200 000-trace per-batch fill did not finish in 120 s.

This is now fixed ([`engine.go`](../internal/sampling/engine.go)):

- **Evaluate only what a batch touched.** Policies are pure functions of a
  trace's spans, so a trace that gained no span cannot change its decision.
  `Consume` collects the traces the batch added spans to and evaluates only
  those; the buffer at large is left alone.
- **Time-ordered expiry.** Traces are also held in a FIFO list in arrival order.
  Since every trace shares one `DecisionWait`, arrival order is deadline order, so
  expiry pops the front while it is due and stops at the first trace that is not —
  O(k) in the number actually expiring, not O(n). A trace decided early is
  unlinked in O(1) via the `*list.Element` it holds.

The same per-batch fill now runs **1 000 000 traces in ~2.9 s** (linear).
Regression tests cover it: a trace whose keep-worthy span arrives in a later
batch is still sampled (`TestLateErrorSpanSamplesBufferedTrace`), and the id
index and arrival list stay in lockstep with no leaked nodes
(`TestBufferIndexAndOrderStayConsistent`).

Cost of the fix: one extra allocation per new trace (the list node), visible as
4 → 5 allocs/op in `BenchmarkEngineConsume` — a deliberate trade for O(1)
unlinking and O(k) expiry. What the hot-path `TODO(core)` still leaves open is a
sharded/lock-striped buffer (mutex contention) and smarter eviction than
drop-newest at the `MaxTraces` cap.

## Next milestone: head-to-head vs the stock `tail_sampling` processor

The numbers above are Astriena's **own** baseline. To make the "fraction of the
memory" claim a *ratio*, the same workload must run through the stock
`tailsamplingprocessor` from opentelemetry-collector-contrib and be measured the
same way. That comparison cannot live in this module: the stock processor needs
the Collector runtime and `pdata`, which the root module deliberately does not
depend on (that dependency-freedom is what keeps this engine benchmarkable in the
first place).

Planned shape:

- A separate benchmark module (e.g. `bench/` with its own `go.mod`, or under
  `components/`) that imports both the contrib `tailsamplingprocessor` and
  Astriena's `astriena_sampler` adapter.
- Feed both the *same* generated `ptrace.Traces` batches (port `genBatches` to
  emit pdata) under an identical keep policy set.
- Measure steady-state `HeapInuse` at a fixed in-flight trace count, plus
  allocs/op and throughput, for both — reporting the **ratio**.

Until that lands, this page proves the drop-rate claim outright and establishes
Astriena's absolute memory/throughput baseline; the memory *comparison* is
explicitly still open.
