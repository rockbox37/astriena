// Package clickhouse writes sampled telemetry into a customer-owned ClickHouse
// cluster — Astriena's Bring Your Own Storage (BYOS) model. Data never touches
// Astriena's infrastructure.
//
// Like the sampling engine, this package is framework-free: the Collector
// exporter adapter in components/clickhouseexporter wraps it. The BYOS
// differentiators live here — auto-schema migration (detect new attribute keys
// and add sparse columns + bloom-filter indexes) and batched, compressed writes.
package clickhouse

import (
	"context"
	"time"
)

// Config describes the customer's own ClickHouse target and write behavior.
type Config struct {
	DSN      string // e.g. clickhouse://user:pass@host:9000/db (BYOS: customer-owned)
	Database string
	Table    string

	BatchSize     int
	FlushInterval time.Duration
}

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

// Writer batches Rows and inserts them into ClickHouse.
//
// TODO(core): wire the official clickhouse-go driver, implement async batching
// bounded by BatchSize/FlushInterval, and add AutoSchema (ALTER TABLE to add
// sparse columns + bloom-filter indexes when new attribute keys appear). No
// driver is imported yet so this pure package builds offline.
type Writer struct {
	cfg Config
	// conn driver.Conn // TODO(core)
}

// NewWriter opens a writer for the given target.
func NewWriter(cfg Config) (*Writer, error) {
	// TODO(core): open the connection and ensure the database/table exist.
	return &Writer{cfg: cfg}, nil
}

// Write inserts a batch of rows.
func (w *Writer) Write(ctx context.Context, rows []Row) error {
	// TODO(core): buffer and batch-insert with compression.
	_ = ctx
	_ = rows
	return nil
}

// Close flushes and closes the writer.
func (w *Writer) Close() error { return nil }
