package clickhouse

import (
	"context"
	"errors"
	"testing"
)

// fakeInserter records calls and can be programmed to fail the next insert.
type fakeInserter struct {
	ensured    int
	addedKeys  [][]string
	batches    [][]Row
	closed     bool
	failInsert error // returned once, then cleared
}

func (f *fakeInserter) EnsureBaseSchema(context.Context) error { f.ensured++; return nil }

func (f *fakeInserter) AddColumns(_ context.Context, keys []string) error {
	cp := append([]string(nil), keys...)
	f.addedKeys = append(f.addedKeys, cp)
	return nil
}

func (f *fakeInserter) InsertBatch(_ context.Context, rows []Row) error {
	if f.failInsert != nil {
		err := f.failInsert
		f.failInsert = nil
		return err
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

func rowWithAttrs(attrs map[string]string) Row {
	return Row{TraceID: "t", SpanID: "s", Attributes: attrs}
}

func TestFlushesWhenBatchSizeReached(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 3})
	ctx := context.Background()

	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(ins.batches) != 0 {
		t.Fatalf("under BatchSize: got %d batches, want 0", len(ins.batches))
	}
	// Buffer reaches 4 (>=3): one flush of all buffered rows.
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
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
	if len(ins.batches) != 0 {
		t.Fatalf("failed insert should record no batch, got %d", len(ins.batches))
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
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write 3: %v", err)
	}
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}, {}}); err != nil {
		t.Fatalf("Write 4: %v", err)
	}
	ins.failInsert = errors.New("still down")
	if err := w.Write(ctx, []Row{{}}); err != errBufferFull {
		t.Fatalf("Write past cap: got %v, want errBufferFull", err)
	}
}

func TestWriteRetriesLeftoverBeforeRejecting(t *testing.T) {
	w, ins := newTestWriter(t, Config{BatchSize: 2})
	ins.failInsert = errors.New("clickhouse unavailable")
	ctx := context.Background()

	if err := w.Write(ctx, make([]Row, 20)); err != nil {
		t.Fatalf("oversized Write: %v", err)
	}
	// Leftover is 20 > cap 8. The next Write must flush that leftover rather
	// than reject without retrying.
	if err := w.Write(ctx, []Row{{}}); err != nil {
		t.Fatalf("Write after leftover: %v", err)
	}
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
	if len(ins.batches) != 1 || len(ins.batches[0]) != 20 {
		t.Fatalf("oversized Write: got %v, want 1 batch of 20", sizes(ins.batches))
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
