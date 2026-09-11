# Benchmarks — proving the wedge

Astriena's pitch rests on two measurable claims:

- **"~80% ingestion reduction"** — dropping redundant healthy traces while keeping
  100% of errors and latency spikes.
- **Cheaper per-call ingest than the stock processor** — fewer allocations and
  less garbage per `ConsumeTraces` (63 vs 82 allocs/op). Wall-clock latency has
  measured lower too, but see the timings note below before citing a ratio.

A third figure is measured but **not** sold as an advantage: in-flight memory.
The engine's own bookkeeping is ~210–230 B/trace in isolation, and at the adapter
boundary — where span payloads dominate — heap is a tie with stock
(~0.97–1.02×). The earlier *"a fraction of the memory of the stock processor"*
claim was retired from the README, the architecture doc and `builder-config.yaml`
on the strength of the numbers below.

This document records how the two claims above are measured, and the numbers as
of the last run.
[`architecture.md`](architecture.md) calls for exactly this — *"benchmark the
engine in isolation"* and *"benchmark this package against the stock
`tail_sampling` processor before optimizing"* — so these numbers are the baseline
that must exist **before** any hot-path optimization of
[`internal/sampling/engine.go`](../internal/sampling/engine.go) is attempted.

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

Apple M4, `go test`, `-benchtime=200000x`, re-measured at `main` @ `8063374`
(post-#36). Reproduce with `make bench`; absolute
numbers are hardware-dependent — the ratios and the shape are the point.

> **Timings pending re-measurement.** The allocation counts here are
> deterministic, and the heap figures were re-measured alongside them (they stay
> noise-sensitive — re-run at the documented `-benchtime=200000x` and take a
> median across runs). Both are current
> (re-measured after the O(1) pending-eviction index,
> #36, which added one `list.Element` per buffered trace: 5 → 6 allocs/op and
> ~161 → ~211 B/trace). The **ns/op** figures are not current — the machine
> available when #36 landed was too loaded to time reliably, and a wall-clock
> figure measured under load can be wrong by 2–3× in a way that looks stable
> across `-count` samples. Re-run `make bench`, `make bench-h2h` and
> `make bench-h2h-concurrent` on an idle machine and update the ns/op columns
> together. Prefer interleaved A/B runs across the two commits over running all
> of one side then all of the other.

| Benchmark | Metric | Value |
|---|---|---|
| `BenchmarkEngineConsume` | throughput | see the note above — needs re-measurement |
| | allocations | 6 allocs/op, 248 B/op |
| | **realized drop rate** | **79.99 %** |
| `BenchmarkEngineBufferBytes` | **engine bookkeeping per buffered trace** | **~210–230 heap B/trace** (varies with N as the map grows/fragments) |

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
its node in the arrival list *and* its node in the pending-only list (two
`*list.Element`s since #36), and the per-trace span-pointer slice.

It **excludes** the span payloads and attribute maps — those are allocated by the
workload generator *before* the heap is sampled, so they do not count. This is
deliberate: the span payload exists in any processor and, in the real adapter, is
dominated by the opaque `Span.Raw` pdata the exporter will write (see
`toEngineSpans`). What this benchmark isolates is the per-trace
bookkeeping overhead Astriena controls. The head-to-head below measures a
different quantity — adapter-level in-flight heap, where payloads dominate — and
is where the cross-processor ratio lives.

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
the pending-only list (`pendingOrder`, since #36) rather than dropping the
incoming span when room can be made.
Isolation bench drop rate is unchanged at `MaxTraces=0`. Allocation counts are
**not**: `pendingOrder.PushBack` is unconditional, so an unlimited buffer pays
the extra node (and 48 B) for a list that nothing reads — see the note at the end of
this document. Re-run
`make bench` after changing cap/eviction behavior under load.

## Head-to-head vs the stock `tail_sampling` processor

The isolation numbers above are Astriena's **own** engine baseline. Any
cross-processor claim is a *ratio*, so the same workload has to run
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
span payloads. Isolation's ~210–230 B/trace deliberately excluded those
payloads; they dominate here.

### Results (adapter-level)

Representative local run on **Apple M4**, `go test -C bench -benchtime=20000x
-count=5`. The allocation and heap rows were re-measured at `main` @ `8063374`
(post-#36); the withheld ns/op figures were taken at `main` @ `c44b685`. Reproduce with `make bench-h2h` (add `-count=5` for
stable numbers). Absolute ns/op and heap figures vary by machine, Go version, and
thermal state — **the ratios and the shape are the point**, not reproducing these
exact integers.

| Benchmark | Processor | Metric | Value |
|---|---|---|---|
| `BenchmarkBufferBytes` | Astriena | **heap B/trace** | **~3430–3510** |
| | stock `tail_sampling` | **heap B/trace** | **~3345–3510** |
| | | **ratio (stock / Astriena)** | **~0.97–1.02×** (a tie within noise) |
| `BenchmarkConsume` | Astriena | allocations | 3983 B/op, 63 allocs/op |
| | stock `tail_sampling` | allocations | 4914 B/op, 82 allocs/op |
| | | **allocation ratio (stock / Astriena)** | **~1.30× allocs, ~1.23× bytes** |
| | | ingest latency ratio | pending re-measurement (see note) |

Earlier runs on the same machine (pre lock-striping, PR #12 era) showed ~2.25×
ingest advantage (~2061 vs ~4636 ns/op). That gap narrowed after **64-stripe lock
striping** (#16): single-thread ingest pays ~10–15% overhead (+5 allocs/op) for
finer-grained locking the h2h bench cannot exercise. The h2h benchmarks are
**single-goroutine** — they measure per-call ingest cost, not mutex contention
wins under concurrent load.

### What the ratio means (and does not)

At the processor boundary, in-flight heap is a **tie**. The former README /
architecture claim of "a fraction of the memory of the stock processor" was
**not supported** by this run — and has since been removed from both documents in
favor of the ingestion-reduction and ingest-allocation claims. Astriena's adapter
holds a domain `Span` *and* a per-trace pdata
snapshot (`toEngineSpans`), and that dual representation lands at essentially
the same `HeapInuse` as stock's own copies + `idToTrace` map. The memory ratio
(~0.97–1.02×) is unchanged post-striping.

The isolation engine bookkeeping (~210–230 B/trace) is ~6% of the ~3450 B
adapter-level figure. The payload the exporter will write dominates both sides.
A 1024-trace smoke sample can invert the ratio (heap noise); it stabilizes
near 1.00× by 20 000 traces.

Ingest is where Astriena is ahead today, and the durable part of that is
allocation cost: **63 vs 82 allocs/op** and **~3983 vs ~4914 B/op** — counts
reproduce exactly run to run, bytes to within a byte. Wall-clock latency has measured lower as well, but
every recorded ns/op ratio here predates the #36 index and was taken on hardware
that is no longer trusted for timing — do not cite the single-thread ~1.4× (or
the older ~2.25×) as a current ratio until the re-run in the timings note lands.
The historical figures above and the caveated concurrent ratio column below are
labelled as such.

Lock-striping / smarter eviction (landed in #8, striping refined in #16) target
mutex contention and cap behavior under concurrent ingest, not this memory ratio.
A Rust hot path is also still not justified: this run does not show Go GC / memory
losing to stock at the processor boundary. If a later customer-scale run does,
that is the trigger; do not start a rewrite on the back of these numbers.

## Concurrent ingest (head-to-head)

The single-goroutine h2h bench above measures per-call ingest cost. It does **not**
exercise the 64-stripe lock striping added for concurrent OTLP ingest (#8, #16).
`BenchmarkConcurrentConsume` closes that gap: N worker goroutines share one
processor instance and call `ConsumeTraces` in parallel with the same synthetic
workload and keep policy as `BenchmarkConsume`.

### Running them

```sh
make bench-h2h-concurrent
# or, for stable numbers (repeat 5 times):
go test -C bench -run '^$' -bench BenchmarkConcurrentConsume -benchmem -benchtime=20000x -count=5
```

Sub-benchmarks run at worker counts **1, 4, 8**, and **`runtime.NumCPU()`**
(deduplicated — e.g. on an 8-core machine you get three levels, not four).
Each sub-benchmark sets **`GOMAXPROCS` to the worker count** and uses
`testing.B.RunParallel` with matching parallelism so OS scheduling matches the
intended load shape.

### Workload and methodology

Same as the single-thread h2h: 4096-batch pool, 4 spans/trace, 20% keep-worthy,
unique trace ids per iteration, 10 000 in-flight cap. Each parallel worker **clones**
the batch before rewriting the trace id (the single-thread bench reuses batches
in place; concurrent access requires independent pdata). Both processors pay the
same clone cost, so the ratio remains apples-to-apples. Stock is drained after
each sub-benchmark as in `BenchmarkConsume`.

### What is measured

**`BenchmarkConcurrentConsume/workers=N/{astriena,stock}`** — aggregate ingest
throughput under N-way parallel `ConsumeTraces`: ns/op, B/op, allocs/op. Compare
**workers=1** to higher counts to see whether Astriena's striping holds or improves
its advantage as contention rises; compare **stock / Astriena** at each N for the
ratio under load.

### Results (concurrent adapter-level)

Representative local run on **Apple M4** (10 cores), `main`, `-benchtime=20000x
-count=5`. Reproduce with `make bench-h2h-concurrent` (add `-count=5` for stable
numbers). Absolute figures vary by machine — **the shape across worker counts
and the stock/Astriena ratio at each N are the point**.

> **The ns/op and ratio columns below share the timings caveat above** — they
> predate the #36 index and were taken on hardware no longer trusted for timing.
> The allocs/op column is deterministic; B/op drifts by a few bytes run to run.
> Both are current.

| Workers | Processor | ns/op (median) | B/op | allocs/op | stock / Astriena |
|---:|---|---:|---:|---:|---:|
| 1 | Astriena | ~15 000 | ~6610 | 104 | — |
| 1 | stock | ~16 400 | ~7540 | 123 | ~1.1× |
| 4 | Astriena | ~6900 | ~6610 | 104 | — |
| 4 | stock | ~5600 | ~7540 | 123 | ~0.8× |
| 8 | Astriena | ~5100 | ~6610 | 104 | — |
| 8 | stock | ~5000 | ~7540 | 123 | ~1.0× |
| 10 (NumCPU) | Astriena | ~5800 | ~6610 | 104 | — |
| 10 (NumCPU) | stock | ~8400 | ~7540 | 123 | ~1.4× |

**Clone overhead.** Each parallel iteration **clones** the batch before
rewriting the trace id (see methodology above). That adds ~41 allocs/op versus
the single-thread `BenchmarkConsume` figures (104 vs 63), and dominates at
**workers=1**, where there is no contention for it to hide behind. Both
processors pay the same
clone, so cross-processor ratios remain fair.

**Scaling shape.** As workers increase, **aggregate ns/op falls** on both sides
— parallel ingest amortizes per-call work. At **4–8 workers** on this machine,
stock's async `workChan` loop keeps pace with Astriena (ratios near 1.0×); the
single-thread ingest advantage **narrows or inverts** at moderate concurrency.
At **NumCPU (10)**, Astriena pulls ahead again, by a margin pending
re-measurement, while stock's tail is
noisy (workChan backpressure and event-loop batching vary run-to-run).

**How to read this.** The bench proves concurrent load can be measured
reproducibly and shows striping does not regress under parallel ingest. It does
**not** by itself prove a large striping win — stock's async ingest also scales.
Use **workers=1** concurrent numbers only for apples-to-apples clone-inclusive
comparison; use **single-thread `BenchmarkConsume`** for per-call ingest cost
without clone. Re-run after engine or adapter changes that touch locking or
ingest paths.

## Open observation: `pendingOrder` is unconditional

`Engine.ingestSpan` pushes every new trace onto `pendingOrder` regardless of
configuration. With `MaxTraces = 0` (unlimited) `evictOldestPendingLocked` can
never run, so that node — one allocation and ~48 B per buffered trace, about a
quarter of engine bookkeeping — is maintained for a list that nothing reads. Guarding
the push on `cfg.MaxTraces > 0` would restore the pre-#36 ~161 B/trace figure for
unlimited buffers at no cost to the capped path. Not yet implemented; noted here
because it is the reason the isolation numbers moved.
