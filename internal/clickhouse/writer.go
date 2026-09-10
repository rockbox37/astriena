// Package clickhouse writes sampled telemetry into a customer-owned ClickHouse
// cluster — Astriena's Bring Your Own Storage (BYOS) model. Data never touches
// Astriena's infrastructure.
//
// Like the sampling engine, this package is framework-free and builds offline:
// it holds the batching and auto-schema *logic* but imports no ClickHouse driver.
// The network binding lives behind the Inserter port and is supplied by the
// Collector exporter adapter (components/clickhouseexporter). Keeping the driver
// out preserves the root module's "no heavy deps, builds offline" invariant (see
// docs/architecture.md) and lets this logic be unit-tested with a fake Inserter.
package clickhouse

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Config describes write-batching behavior. Connection identity (DSN, database,
// table) lives on the Inserter, not here.
type Config struct {
	// BatchSize is the row count that triggers a flush. <=0 flushes every Write
	// (no size-based batching).
	BatchSize int
	// FlushInterval bounds how long buffered rows wait before a time-based flush.
	// <=0 disables the background flush ticker (size- and Close-triggered only).
	FlushInterval time.Duration
}

var (
	errWriterClosed = errors.New("clickhouse: writer closed")
	errBufferFull   = errors.New("clickhouse: write buffer full")
)

// Row is one span flattened for insertion. The exporter builds these from the
// engine's sampled spans.
type Row struct {
	Timestamp  time.Time
	TraceID    string
	SpanID     string
	Name       string
	StatusCode string
	DurationNS int64
	Attributes map[string]string
}

// Inserter is the port the real ClickHouse driver implements. It is the only
// seam that touches the network; everything above it (buffering, batching,
// auto-schema diffing) is pure and lives in Writer.
type Inserter interface {
	// EnsureBaseSchema creates the database/table with Astriena's fixed columns
	// if they do not exist. Called once before the first insert.
	EnsureBaseSchema(ctx context.Context) error
	// AddColumns materializes a sparse, bloom-filter-indexed column for each new
	// attribute key so high-cardinality metadata stays queryable. Must be
	// idempotent (ADD COLUMN / ADD INDEX IF NOT EXISTS): Writer may re-request a
	// key across concurrent flushes.
	AddColumns(ctx context.Context, keys []string) error
	// InsertBatch writes a batch of rows (compressed) into the target table.
	InsertBatch(ctx context.Context, rows []Row) error
	// Close releases the underlying connection.
	Close() error
}

// Stats holds per-reason counters for flush retries and write rejection. Values
// are monotonic for the lifetime of the Writer.
type Stats struct {
	FlushAttempts            uint64
	FlushFailures            uint64
	RowsRebuffered           uint64
	WritesRejectedBufferFull uint64
}

// Writer batches Rows and flushes them through an Inserter. It is safe for
// concurrent use.
//
// Flushes are triggered three ways: when the buffer reaches BatchSize, on the
// FlushInterval ticker (for partial batches under light load), and on Close.
// All flushes run on a dedicated background worker so Write does not block on
// ClickHouse I/O. Before a batch is inserted, any attribute keys not yet seen
// are pushed through AddColumns — the auto-schema path — so the sparse columns
// exist before the rows referencing them land.
type Writer struct {
	cfg Config
	ins Inserter

	mu      sync.Mutex
	buf     []Row
	known   map[string]bool // attribute keys already ensured via AddColumns
	closed  bool
	stopped bool

	flushCh    chan flushJob
	workerDone chan struct{}
	flushMu    sync.Mutex // serializes flushCh send with Close

	writers sync.WaitGroup // in-flight Write calls

	stop chan struct{}
	done chan struct{}

	stats stats
}

type stats struct {
	flushAttempts            atomic.Uint64
	flushFailures            atomic.Uint64
	rowsRebuffered           atomic.Uint64
	writesRejectedBufferFull atomic.Uint64
}

type flushJob struct {
	ctx   context.Context
	batch []Row // nil: drain whatever is currently buffered
	done  chan struct{}
	errCh chan error // receives insertBatch result when the caller waits
	idle  bool       // wait-for-drain sentinel; no I/O
}

const flushQueueDepth = 4

// NewWriter constructs a Writer that flushes through ins. Call Start to ensure
// the base schema and begin the flush ticker, and Close to drain and release.
func NewWriter(cfg Config, ins Inserter) (*Writer, error) {
	w := &Writer{
		cfg:        cfg,
		ins:        ins,
		known:      make(map[string]bool),
		flushCh:    make(chan flushJob, flushQueueDepth),
		workerDone: make(chan struct{}),
	}
	go w.runWorker()
	return w, nil
}

// Stats returns a snapshot of writer counters.
func (w *Writer) Stats() Stats {
	return Stats{
		FlushAttempts:            w.stats.flushAttempts.Load(),
		FlushFailures:            w.stats.flushFailures.Load(),
		RowsRebuffered:           w.stats.rowsRebuffered.Load(),
		WritesRejectedBufferFull: w.stats.writesRejectedBufferFull.Load(),
	}
}

func (w *Writer) runWorker() {
	defer close(w.workerDone)
	for job := range w.flushCh {
		w.executeFlush(job)
	}
}

func (w *Writer) executeFlush(job flushJob) {
	var err error
	defer func() {
		if job.errCh != nil {
			job.errCh <- err
		}
		if job.done != nil {
			close(job.done)
		}
	}()
	if job.idle {
		return
	}
	w.stats.flushAttempts.Add(1)
	if err = w.insertBatch(job.ctx, job.batch); err != nil {
		w.stats.flushFailures.Add(1)
	}
}

// Start ensures the base schema exists and, when FlushInterval is set, launches
// the background flush ticker. It is a no-op ticker-wise if already started.
func (w *Writer) Start(ctx context.Context) error {
	if err := w.ins.EnsureBaseSchema(ctx); err != nil {
		return err
	}
	w.mu.Lock()
	if w.cfg.FlushInterval <= 0 || w.stop != nil || w.closed {
		w.mu.Unlock()
		return nil
	}
	w.stopped = false
	w.stop = make(chan struct{})
	w.done = make(chan struct{})
	w.mu.Unlock()
	go func() {
		defer close(w.done)
		t := time.NewTicker(w.cfg.FlushInterval)
		defer t.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
				w.mu.Lock()
				if w.closed {
					w.mu.Unlock()
					continue
				}
				w.mu.Unlock()
				// Best-effort: if the worker queue is saturated, rows stay
				// buffered until the next tick or size-triggered flush.
				_ = w.enqueueFlush(context.Background(), nil, false)
			}
		}
	}()
	return nil
}

// Write buffers rows and enqueues a flush once the buffer reaches BatchSize.
// When the buffer is at its cap with a failed-batch leftover, Write blocks on
// a synchronous flush before accepting more rows or returning errBufferFull.
func (w *Writer) Write(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errWriterClosed
	}
	w.writers.Add(1)
	w.mu.Unlock()
	defer w.writers.Done()
	w.mu.Lock()
	// Reject only when a retry leftover is already occupying the cap. An
	// oversized single Write with an empty buffer is flushed through so a
	// Collector batch larger than BatchSize*4 is not rejected forever.
	if limit := w.bufCapLocked(); limit > 0 && len(w.buf) > 0 && len(w.buf)+len(rows) > limit {
		w.mu.Unlock()
		// Retry the leftover first so a failed oversized flush does not stall
		// ingest until the ticker (or forever when FlushInterval is unset).
		// Writer owns retry: flush errors are re-buffered; only errBufferFull
		// signals the Collector when the cap is still exceeded.
		if err := w.enqueueFlush(ctx, nil, true); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return errWriterClosed
		}
		if limit := w.bufCapLocked(); limit > 0 && len(w.buf) > 0 && len(w.buf)+len(rows) > limit {
			w.stats.writesRejectedBufferFull.Add(1)
			w.mu.Unlock()
			return errBufferFull
		}
	}
	w.buf = append(w.buf, rows...)
	var batch []Row
	ready := w.cfg.BatchSize <= 0 || len(w.buf) >= w.cfg.BatchSize
	if ready {
		batch = w.buf
		w.buf = nil
	}
	w.mu.Unlock()
	if batch != nil {
		if err := w.enqueueFlush(ctx, batch, false); err != nil {
			w.rebuffer(batch)
			return err
		}
	}
	return nil
}

// enqueueFlush sends a flush job to the worker. When wait is true the caller
// blocks until the job completes.
func (w *Writer) enqueueFlush(ctx context.Context, batch []Row, wait bool) error {
	jobCtx := ctx
	if !wait {
		// Async flushes outlive the Write caller; do not inherit a cancelable ctx.
		jobCtx = context.Background()
	}
	job := flushJob{ctx: jobCtx, batch: batch}
	if wait {
		job.done = make(chan struct{})
		job.errCh = make(chan error, 1)
	}
	w.flushMu.Lock()
	select {
	case w.flushCh <- job:
	case <-ctx.Done():
		w.flushMu.Unlock()
		return ctx.Err()
	}
	w.flushMu.Unlock()
	if !wait {
		return nil
	}
	select {
	case err := <-job.errCh:
		<-job.done
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sync waits until all prior flush jobs complete without starting a new flush.
func (w *Writer) sync(ctx context.Context) error {
	job := flushJob{idle: true, done: make(chan struct{})}
	w.flushMu.Lock()
	select {
	case w.flushCh <- job:
	case <-ctx.Done():
		w.flushMu.Unlock()
		return ctx.Err()
	}
	w.flushMu.Unlock()
	select {
	case <-job.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// bufCapLocked is the hard memory bound on buffered rows. A failed flush is
// re-buffered even if it exceeds this; subsequent Writes are rejected until
// a flush succeeds. When BatchSize is unset, use the factory default size.
func (w *Writer) bufCapLocked() int {
	if w.cfg.BatchSize > 0 {
		return w.cfg.BatchSize * 4
	}
	return 10_000
}

// insertBatch ensures attribute columns and inserts batch. When batch is nil the
// current buffer is taken under lock. On failure the batch is re-buffered.
func (w *Writer) insertBatch(ctx context.Context, batch []Row) error {
	if batch == nil {
		w.mu.Lock()
		if len(w.buf) == 0 {
			w.mu.Unlock()
			return nil
		}
		batch = w.buf
		w.buf = nil
		w.mu.Unlock()
	}

	newKeys := w.newKeys(batch)
	if len(newKeys) > 0 {
		if err := w.ins.AddColumns(ctx, newKeys); err != nil {
			w.rebuffer(batch)
			return err
		}
		w.mu.Lock()
		for _, k := range newKeys {
			w.known[k] = true
		}
		w.mu.Unlock()
	}

	if err := w.ins.InsertBatch(ctx, batch); err != nil {
		w.rebuffer(batch)
		return err
	}
	return nil
}

// newKeys returns the attribute keys in batch not yet ensured, deduped.
func (w *Writer) newKeys(batch []Row) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	seen := make(map[string]bool)
	for _, r := range batch {
		for k := range r.Attributes {
			if w.known[k] || seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// rebuffer prepends a failed batch back onto the buffer so no rows are lost on a
// transient error. Ordering relative to concurrent writes is not preserved.
func (w *Writer) rebuffer(batch []Row) {
	w.stats.rowsRebuffered.Add(uint64(len(batch)))
	w.mu.Lock()
	w.buf = append(batch, w.buf...)
	w.mu.Unlock()
}

// Close stops the flush ticker, drains any buffered rows, and closes the
// Inserter. ctx cancels only the wait for the ticker; the drain always runs
// so rows Write already accepted are not dropped.
func (w *Writer) Close(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	stop, done := w.stop, w.done
	if stop != nil && !w.stopped {
		w.stopped = true
		close(stop)
	}
	w.mu.Unlock()

	var waitErr error
	if stop != nil {
		select {
		case <-done:
			w.mu.Lock()
			w.stop = nil
			w.mu.Unlock()
		case <-ctx.Done():
			waitErr = ctx.Err()
		}
	}

	// Wait for in-flight Write calls to finish enqueueing so Close does not
	// close flushCh while a caller still holds a detached batch.
	w.writers.Wait()

	var flushErr error
	for {
		// Wait for in-flight async flushes before treating the buffer as empty.
		// A size-triggered batch may still be inserting (or re-buffering on
		// failure) after writers.Wait returns.
		if err := w.sync(context.Background()); err != nil {
			flushErr = err
			break
		}
		w.mu.Lock()
		empty := len(w.buf) == 0
		w.mu.Unlock()
		if empty {
			break
		}
		err := w.enqueueFlush(context.Background(), nil, true)
		if err != nil {
			flushErr = err
			break
		}
	}

	w.flushMu.Lock()
	close(w.flushCh)
	w.flushMu.Unlock()
	<-w.workerDone

	closeErr := w.ins.Close()
	if waitErr != nil {
		return waitErr
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
