package clickhouse

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeInserter records calls and can be programmed to fail the next insert.
type fakeInserter struct {
	mu         sync.Mutex
	ensured    int
	addedKeys  [][]string
	batches    [][]Row
	closed     bool
	failInsert    error         // returned once, then cleared
	insertWait    chan struct{} // when set, InsertBatch blocks until closed
	insertStarted chan struct{} // closed on first InsertBatch entry (before insertWait)
	insertOnce    sync.Once
}

func (f *fakeInserter) EnsureBaseSchema(context.Context) error { f.ensured++; return nil }

func (f *fakeInserter) AddColumns(_ context.Context, keys []string) error {
	cp := append([]string(nil), keys...)
	f.addedKeys = append(f.addedKeys, cp)
	return nil
}

func (f *fakeInserter) InsertBatch(_ context.Context, rows []Row) error {
	f.mu.Lock()
	wait := f.insertWait
	started := f.insertStarted
	fail := f.failInsert
	if f.failInsert != nil {
		f.failInsert = nil
	}
	f.mu.Unlock()

	if started != nil {
		f.insertOnce.Do(func() { close(started) })
	}
	if wait != nil {
		<-wait
	}
	if fail != nil {
		return fail
	}
	f.batches = append(f.batches, append([]Row(nil), rows...))
	return nil
}

func (f *fakeInserter) Close() error { f.closed = true; return nil }

func newTestWriter(t *testing.T, cfg Config) (*Writer, *fakeInserter) {
	t.Helper()
	ins := &fakeInserter{}
	w, err := NewWriter(cfg, ins)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w, ins
}

func waitIdle(t *testing.T, w *Writer, ctx context.Context) {
	t.Helper()
	if err := w.sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func rowWithAttrs(attrs map[string]string) Row {
	return Row{TraceID: "t", SpanID: "s", Attributes: attrs}
}

func TestFlushesWhenBatchSizeReached(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 3})
	ctx := context.Background()

	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitIdle(t, w, ctx)
	if len(ins.batches) != 0 {
		t.Fatalf("under BatchSize: got %d batches, want 0", len(ins.batches))
	}
	// Buffer reaches 4 (>=3): one flush of all buffered rows.
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitIdle(t, w, ctx)
	if len(ins.batches) != 1 || len(ins.batches[0]) != 4 {
		t.Fatalf("at BatchSize: got %d batches (first size %v), want 1 batch of 4",
			len(ins.batches), sizes(ins.batches))
	}
}

func TestAutoSchemaOnlyAddsNewKeys(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 1}) // flush every write
	ctx := context.Background()

	if err := w.Write(ctx, []Row{rowWithAttrs(map[string]string{"a": "1", "b": "2"})}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Write(ctx, []Row{rowWithAttrs(map[string]string{"a": "9", "c": "3"})}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitIdle(t, w, ctx)

	if len(ins.addedKeys) != 2 {
		t.Fatalf("AddColumns called %d times, want 2", len(ins.addedKeys))
	}
	if got := asSet(ins.addedKeys[0]); !got["a"] || !got["b"] || len(got) != 2 {
		t.Errorf("first AddColumns = %v, want {a,b}", ins.addedKeys[0])
	}
	// "a" was already ensured, so only the genuinely new "c" is requested.
	if got := ins.addedKeys[1]; len(got) != 1 || got[0] != "c" {
		t.Errorf("second AddColumns = %v, want [c] only (a already known)", got)
	}
}

func TestCloseDrainsAndClosesInserter(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 100}) // never auto-flushes
	ctx := context.Background()

	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(ins.batches) != 0 {
		t.Fatalf("before Close: got %d batches, want 0 (below BatchSize)", len(ins.batches))
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(ins.batches) != 1 || len(ins.batches[0]) != 2 {
		t.Fatalf("Close should flush the partial batch: got %v", sizes(ins.batches))
	}
	if !ins.closed {
		t.Errorf("Close did not close the Inserter")
	}
}

func TestInsertErrorRebuffersForRetry(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ins.failInsert = errors.New("clickhouse unavailable")
	ctx := context.Background()

	// Writer owns retry: a failed flush is re-buffered and Write returns nil so
	// a Collector re-Consume cannot append the same rows a second time.
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write should accept rows when the writer owns retry: %v", err)
	}
	waitIdle(t, w, ctx)
	if len(ins.batches) != 0 {
		t.Fatalf("failed insert should record no batch, got %d", len(ins.batches))
	}
	if st := w.Stats(); st.FlushFailures != 1 || st.RowsRebuffered != 2 {
		t.Fatalf("stats after failed flush: %+v", st)
	}
	// The rows must not be lost: a subsequent flush (driver recovered) inserts them.
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close after recovery: %v", err)
	}
	if len(ins.batches) != 1 || len(ins.batches[0]) != 2 {
		t.Fatalf("re-buffered rows not flushed on recovery: got %v", sizes(ins.batches))
	}
}

func TestStartEnsuresBaseSchema(t *testing.T) {
	w, ins := newTestWriter(t, Config{}) // FlushInterval 0: no ticker
	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if ins.ensured != 1 {
		t.Fatalf("EnsureBaseSchema called %d times, want 1", ins.ensured)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWriteAfterCloseRejected(t *testing.T) {
	w, _ := newTestWriter(t, Config{BatchSize: 100})
	ctx := context.Background()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Write(ctx, []Row{{}}); err != errWriterClosed {
		t.Fatalf("Write after Close: got %v, want errWriterClosed", err)
	}
}

func TestWriteRejectsWhenBufferFull(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ins.failInsert = errors.New("clickhouse unavailable")
	ctx := context.Background()

	// Failed flush re-buffers 2 rows; cap is BatchSize*4 = 8. Fill to the cap
	// with subsequent Writes that also fail to flush, then reject the overflow.
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	waitIdle(t, w, ctx)
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	waitIdle(t, w, ctx)
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write 3: %v", err)
	}
	waitIdle(t, w, ctx)
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write 4: %v", err)
	}
	waitIdle(t, w, ctx)
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}}); err != errBufferFull {
		t.Fatalf("Write past cap: got %v, want errBufferFull", err)
	}
	if st := w.Stats(); st.WritesRejectedBufferFull != 1 {
		t.Fatalf("buffer-full metric: got %d, want 1", st.WritesRejectedBufferFull)
	}
}

func TestWriteRetriesLeftoverBeforeRejecting(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ins.failInsert = errors.New("clickhouse unavailable")
	ctx := context.Background()

	if err := w.Write(ctx, make([]Row, 20)); err != nil {
		t.Fatalf("oversized Write: %v", err)
	}
	waitIdle(t, w, ctx)
	// Leftover is 20 > cap 8. The next Write must flush that leftover rather
	// than reject without retrying.
	if err := w.Write(ctx, []Row{{}}); err != nil {
		t.Fatalf("Write after leftover: %v", err)
	}
	waitIdle(t, w, ctx)
	if len(ins.batches) != 1 || len(ins.batches[0]) != 20 {
		t.Fatalf("leftover not retried: got %v, want 1 batch of 20", sizes(ins.batches))
	}
}

func TestCloseFailedDrainStillClosesInserter(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 100})
	ctx := context.Background()
	if err := w.Write(ctx, []Row{{}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := errors.New("clickhouse unavailable")
	ins.failInsert = want
	if err := w.Close(ctx); !errors.Is(err, want) {
		t.Fatalf("Close: got %v, want %v", err, want)
	}
	if !ins.closed {
		t.Fatal("failed drain must still close the Inserter")
	}
}

func TestCloseCancelledCtxStillDrains(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 100})
	ctx := context.Background()
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Close(cancelled); err != nil {
		t.Fatalf("Close with cancelled ctx: %v", err)
	}
	if len(ins.batches) != 1 || len(ins.batches[0]) != 2 {
		t.Fatalf("cancelled Close must still drain: got %v", sizes(ins.batches))
	}
}

func TestWriteAcceptsOversizedBatchWhenBufferEmpty(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ctx := context.Background()
	// Cap is 8; a single 20-row Write with an empty buffer must still flush.
	rows := make([]Row, 20)
	if err := w.Write(ctx, rows); err != nil {
		t.Fatalf("oversized Write: %v", err)
	}
	waitIdle(t, w, ctx)
	if len(ins.batches) != 1 || len(ins.batches[0]) != 20 {
		t.Fatalf("oversized Write: got %v, want 1 batch of 20", sizes(ins.batches))
	}
}

func TestWriteDoesNotBlockOnInsert(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ctx := context.Background()
	block := make(chan struct{})
	ins.insertWait = block

	done := make(chan struct{})
	go func() {
		if err := w.Write(ctx, []Row{{}, {}}); err != nil {
			t.Errorf("Write: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
		// Write returned while insert is still blocked — flush is async.
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Write blocked on slow InsertBatch; expected async flush worker")
	}
	close(block)
	waitIdle(t, w, ctx)
	if len(ins.batches) != 1 {
		t.Fatalf("expected 1 batch after unblock, got %d", len(ins.batches))
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFlushQueueBackpressureBlocksWrite(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 1})
	ctx := context.Background()
	block := make(chan struct{})
	ins.insertWait = block

	// Saturate the flush queue while the worker is blocked on InsertBatch.
	for i := 0; i < flushQueueDepth+1; i++ {
		if err := w.Write(ctx, []Row{{}}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	slow := make(chan struct{})
	go func() {
		// Next Write must block until queue drains.
		if err := w.Write(ctx, []Row{{}}); err != nil {
			t.Errorf("blocked Write: %v", err)
		}
		close(slow)
	}()

	select {
	case <-slow:
		t.Fatal("Write should block when flush queue is saturated")
	case <-time.After(100 * time.Millisecond):
	}

	close(block)
	waitIdle(t, w, ctx)

	select {
	case <-slow:
	case <-time.After(2 * time.Second):
		t.Fatal("Write did not unblock after queue drained")
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = ins
}

func TestCloseConcurrentWithWriteDoesNotPanic(t *testing.T) {
	for i := 0; i < 200; i++ {
		w, _ := newTestWriter(t, Config{BatchSize: 1})
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = w.Write(ctx, []Row{{}})
			}
		}()
		time.Sleep(50 * time.Microsecond)
		_ = w.Close(ctx)
		wg.Wait()
	}
}

func TestWritePropagatesContextCancelDuringRetryFlush(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ins.failInsert = errors.New("clickhouse unavailable")
	base := context.Background()

	if err := w.Write(base, make([]Row, 20)); err != nil {
		t.Fatalf("oversized Write: %v", err)
	}
	waitIdle(t, w, base)

	ctx, cancel := context.WithCancel(base)
	cancel()
	if err := w.Write(ctx, []Row{{}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write with cancelled ctx: got %v, want context.Canceled", err)
	}
}

func TestCloseWaitsForInFlightAsyncFlush(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})
	ins.insertWait = block
	ins.insertStarted = started

	done := make(chan struct{})
	go func() {
		if err := w.Write(ctx, []Row{{}, {}}); err != nil {
			t.Errorf("Write: %v", err)
		}
		close(done)
	}()
	<-done
	<-started // async flush is in InsertBatch before Close runs

	closeCh := make(chan error, 1)
	go func() {
		closeCh <- w.Close(ctx)
	}()

	close(block)

	if err := <-closeCh; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(ins.batches) != 1 || len(ins.batches[0]) != 2 {
		t.Fatalf("in-flight async batch lost on Close: got %v", sizes(ins.batches))
	}
}

func TestCloseDrainsAfterInFlightFailureRebuffers(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 1})
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})
	ins.insertWait = block
	ins.insertStarted = started
	ins.failInsert = errors.New("transient")

	done := make(chan struct{})
	go func() {
		if err := w.Write(ctx, []Row{{TraceID: "keep"}}); err != nil {
			t.Errorf("Write: %v", err)
		}
		close(done)
	}()
	<-done
	<-started // in-flight insert blocked; Close will wait then retry drain

	closeCh := make(chan error, 1)
	go func() {
		closeCh <- w.Close(ctx)
	}()

	close(block) // first insert fails (failInsert cleared); Close drain retries

	if err := <-closeCh; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(ins.batches) != 1 || len(ins.batches[0]) != 1 || ins.batches[0][0].TraceID != "keep" {
		t.Fatalf("re-buffered row lost on Close: got %v", sizes(ins.batches))
	}
}

func TestWorkerShutdownDrainsPendingFlush(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 100})
	ctx := context.Background()
	if err := w.Write(ctx, []Row{{}, {}, {}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(ins.batches) != 1 || len(ins.batches[0]) != 3 {
		t.Fatalf("shutdown drain: got %v, want 1 batch of 3", sizes(ins.batches))
	}
}

func sizes(bs [][]Row) []int {
	out := make([]int, len(bs))
	for i, b := range bs {
		out[i] = len(b)
	}
	return out
}

func asSet(ks []string) map[string]bool {
	m := make(map[string]bool, len(ks))
	for _, k := range ks {
		m[k] = true
	}
	return m
}
