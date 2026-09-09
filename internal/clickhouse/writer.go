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
	"time"
)

// Config describes write-batching behavior. Connection identity (DSN, database,
// table) lives on the Inserter, not here.
type Config struct {
	// BatchSize is the row count that triggers a synchronous flush. <=0 flushes
	// every Write (no size-based batching).
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

// Writer batches Rows and flushes them through an Inserter. It is safe for
// concurrent use.
//
// Flushes are triggered three ways: synchronously when the buffer reaches
// BatchSize, on the FlushInterval ticker (for partial batches under light
// load), and on Close. Before a batch is inserted, any attribute keys not yet
// seen are pushed through AddColumns — the auto-schema path — so the sparse
// columns exist before the rows referencing them land.
//
// TODO(core): the current design flushes inline on the caller's goroutine and
// re-buffers a failed batch so the writer owns retry (Write returns nil). A
// dedicated worker with bounded backpressure and per-reason drop metrics is
// the next step once the driver binding is exercised against a real cluster.
type Writer struct {
	cfg Config
	ins Inserter

	mu      sync.Mutex
	buf     []Row
	known   map[string]bool // attribute keys already ensured via AddColumns
	closed  bool
	stopped bool
	inFlush sync.WaitGroup

	stop chan struct{}
	done chan struct{}
}

// NewWriter constructs a Writer that flushes through ins. Call Start to ensure
// the base schema and begin the flush ticker, and Close to drain and release.
func NewWriter(cfg Config, ins Inserter) (*Writer, error) {
	return &Writer{
		cfg:   cfg,
		ins:   ins,
		known: make(map[string]bool),
	}, nil
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
				// Account before I/O so Close cannot slip through inFlush==0
				// between unlocking and starting the insert. Skip if Close
				// already marked us closed — do not Add after Close's Wait.
				w.mu.Lock()
				if w.closed {
					w.mu.Unlock()
					continue
				}
				w.inFlush.Add(1)
				w.mu.Unlock()
				// Writer owns retry (re-buffer). A ticker flush must not fail
				// the process or surface to the Collector ingest path.
				_ = w.flushAndDone(context.Background())
			}
		}
	}()
	return nil
}

// Write buffers rows and flushes synchronously once the buffer reaches
// BatchSize. The caller blocking on a full-batch flush is the intended
// backpressure signal to the upstream pipeline.
func (w *Writer) Write(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errWriterClosed
	}
	// Reject only when a retry leftover is already occupying the cap. An
	// oversized single Write with an empty buffer is flushed through so a
	// Collector batch larger than BatchSize*4 is not rejected forever.
	if limit := w.bufCapLocked(); limit > 0 && len(w.buf) > 0 && len(w.buf)+len(rows) > limit {
		// Retry the leftover first so a failed oversized flush does not stall
		// ingest until the ticker (or forever when FlushInterval is unset).
		w.inFlush.Add(1)
		w.mu.Unlock()
		// Writer owns retry: leftover stays in buf. Returning the flush
		// error would make the Collector re-Consume and append the same rows.
		_ = w.flushAndDone(ctx)
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return errWriterClosed
		}
		if limit := w.bufCapLocked(); limit > 0 && len(w.buf) > 0 && len(w.buf)+len(rows) > limit {
			w.mu.Unlock()
			return errBufferFull
		}
	}
	w.buf = append(w.buf, rows...)
	ready := w.cfg.BatchSize <= 0 || len(w.buf) >= w.cfg.BatchSize
	if ready {
		// Account under the lock so Close cannot observe inFlush==0 in the
		// gap after unlock and before InsertBatch.
		w.inFlush.Add(1)
	}
	w.mu.Unlock()
	if ready {
		// Writer owns retry: flush re-buffers on failure. Returning that
		// error would make the Collector re-Consume and append the same rows.
		_ = w.flushAndDone(ctx)
	}
	return nil
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

// flush takes the buffered rows, ensures any new attribute columns exist, and
// inserts the batch. On failure the batch is returned to the buffer so a later
// Write, ticker, or Close can retry — the writer owns that retry.
func (w *Writer) flushAndDone(ctx context.Context) error {
	defer w.inFlush.Done()
	return w.flush(ctx)
}

func (w *Writer) flush(ctx context.Context) error {
	w.mu.Lock()
	if len(w.buf) == 0 {
		w.mu.Unlock()
		return nil
	}
	batch := w.buf
	w.buf = nil
	newKeys := w.newKeysLocked(batch)
	w.mu.Unlock()

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

// newKeysLocked returns the attribute keys in batch not yet ensured, deduped.
// It does not mark them known — that happens only after AddColumns succeeds, so
// a failed schema change is retried. Because it does not mark, two concurrent
// flushes may both report the same key; AddColumns is idempotent to absorb that.
func (w *Writer) newKeysLocked(batch []Row) []string {
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

	// Always wait for in-flight flushes, then drain with Background. A
	// cancelled caller ctx must not skip rows Write already accepted.
	w.inFlush.Wait()

	flushErr := w.flush(context.Background())
	if flushErr != nil {
		if waitErr != nil {
			return waitErr
		}
		return flushErr
	}
	closeErr := w.ins.Close()
	if waitErr != nil {
		return waitErr
	}
	return closeErr
}
