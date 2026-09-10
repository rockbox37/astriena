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

# adapter-level comparison vs stock tail_sampling (separate module; needs network once):
make bench-h2h
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
unlinking and O(k) expiry. The hot path now also uses a **64-stripe lock-striped
trace map** (per-trace ingest contends on `hash(trace_id) % 64` instead of one
global mutex) and **MaxTraces eviction of the oldest still-Pending trace** from
the arrival list rather than dropping the incoming span when room can be made.
Isolation bench drop rate and alloc counts are unchanged at `MaxTraces=0`; re-run
`make bench` after changing cap/eviction behavior under load.

## Head-to-head vs the stock `tail_sampling` processor

The isolation numbers above are Astriena's **own** engine baseline. The
"fraction of the memory" claim is a *ratio*, so the same workload has to run
through the stock `tailsamplingprocessor` (opentelemetry-collector-contrib
v0.160.0, the Collector line this distro pins) and be measured the same way.

That comparison cannot live in the root module: the stock processor needs the
Collector runtime and `pdata`, which the root module deliberately does not
depend on. It lives in [`bench/`](../bench/) — its own `go.mod`, importing both
contrib `tailsamplingprocessor` and Astriena's `astriena_sampler` adapter.

### Running them

```sh
make test-bench          # cheap verification (skips the multi-second DecisionWait test)
make bench-h2h           # full comparison: -benchtime=20000x
# or, for stable numbers (short runs vary; repeat 5 times):
go test -C bench -run '^$' -bench . -benchmem -benchtime=20000x -count=5
# cheap smoke:
go test -C bench -short ./...
```

pdata copies make 200 000 traces heavier than the isolation engine bench; **20 000**
is the documented comparison size. Larger N (e.g. `-benchtime=50000x`) is fine
if you want a quieter heap sample. Use **`-count=5`** (or higher) when comparing
runs — ingest ns/op and heap B/trace can swing noticeably on a single pass.

### Workload and policy (identical on both sides)

Port of `genBatches`: 4 spans/trace, 6 attributes/span, 20% keep-worthy
(`ERROR` or duration ≥ 200 ms). Keep policy set:

| Astriena `astriena_sampler` | Stock `tail_sampling` |
|---|---|
| `status_code` keep=`ERROR` | `status_code` `status_codes: [ERROR]` |
| `latency` `threshold_ms=200` | `latency` `threshold_ms=200` |

Stock is configured with `sample_on_first_match: true` and
`sampling_strategy: span-ingest` so it evaluates on the event loop as batches
arrive — the closest match to Astriena's ingest-time policy pass. The stock
default (`trace-complete`) only decides on a 1 s ticker and would make a keep
check wait seconds and a memory fill race the clock.

`TestPolicyParity` asserts both processors keep the same keep-worthy traces
and do not forward healthy ones inside `DecisionWait`. `TestDropAfterWait`
(skipped under `-short`) waits out `DecisionWait` and checks healthy traces
are dropped, not forwarded.

### What is measured

**`BenchmarkBufferBytes/{astriena,stock}`** — N pending traces (`keepFrac=0`,
`DecisionWait=30s` so nothing expires during the fill). The generated pdata is
allocated *before* the first heap sample, so it cancels out of the delta; what
remains is what each processor copied and indexed. Stock `ConsumeTraces` is
async (copy + `workChan`); the bench drains the event loop before the second
heap sample. Reports `heapB/trace`. The **ratio** is stock ÷ Astriena.

**`BenchmarkConsume/{astriena,stock}`** — ingest path: ns/op, B/op, allocs/op
on the same cycling pdata pool with unique per-iteration trace ids and a 10 000
in-flight cap (eviction bounds the buffer). Stock's number includes the
ResourceSpans copy plus enqueue (and backpressure once `workChan` fills);
Astriena's is a synchronous translate + `Engine.Consume`. They are the same
public `ConsumeTraces` call, not the same internal work.

This is the **adapter** boundary, not the isolation engine: both sides hold
span payloads. Isolation's ~120–180 B/trace deliberately excluded those
payloads; they dominate here.

### Results (adapter-level)

Representative local run on **Apple M4**, `main` @ `c44b685`, `go test -C bench
-benchtime=20000x -count=5`. Reproduce with `make bench-h2h` (add `-count=5` for
stable numbers). Absolute ns/op and heap figures vary by machine, Go version, and
thermal state — **the ratios and the shape are the point**, not reproducing these
exact integers.

| Benchmark | Processor | Metric | Value |
|---|---|---|---|
| `BenchmarkBufferBytes` | Astriena | **heap B/trace** | **~3409** |
| | stock `tail_sampling` | **heap B/trace** | **~3444** |
| | | **ratio (stock / Astriena)** | **~1.00×** |
| `BenchmarkConsume` | Astriena | throughput | ~4006 ns/op, 3935 B/op, 62 allocs/op |
| | stock `tail_sampling` | throughput | ~5647 ns/op, 4915 B/op, 82 allocs/op |
| | | **ingest latency ratio (stock / Astriena)** | **~1.4×** |

Earlier runs on the same machine (pre lock-striping, PR #12 era) showed ~2.25×
ingest advantage (~2061 vs ~4636 ns/op). That gap narrowed after **64-stripe lock
striping** (#16): single-thread ingest pays ~10–15% overhead (+5 allocs/op) for
finer-grained locking the h2h bench cannot exercise. The h2h benchmarks are
**single-goroutine** — they measure per-call ingest cost, not mutex contention
wins under concurrent load.

### What the ratio means (and does not)

At the processor boundary, in-flight heap is a **tie**. The README / architecture
claim of "a fraction of the memory of the stock processor" is **not supported**
by this run: Astriena's adapter holds a domain `Span` *and* a per-trace pdata
snapshot (`toEngineSpans`), and that dual representation lands at essentially
the same `HeapInuse` as stock's own copies + `idToTrace` map. The memory wedge
(~1.00×) is unchanged post-striping.

The isolation engine bookkeeping (~120–180 B/trace) is ~5% of the ~3400 B
adapter-level figure. The payload the exporter will write dominates both sides.
A 1024-trace smoke sample can invert the ratio (heap noise); it stabilizes
near 1.00× by 20 000 traces.

Ingest is where Astriena is ahead today: on representative runs, about **~1.4×**
lower `ConsumeTraces` latency and fewer allocs (62 vs 82). Do not cite the older
~2.25× figure without noting hardware and pre-striping context — ratios vary by
machine and run. That is the public API, with the async-vs-sync caveat above.

Lock-striping / smarter eviction (landed in #8, striping refined in #16) target
mutex contention and cap behavior under concurrent ingest, not this memory ratio.
A Rust hot path is also still not justified: this run does not show Go GC / memory
losing to stock at the processor boundary. If a later customer-scale run does,
that is the trigger; do not start a rewrite on the back of these numbers.
