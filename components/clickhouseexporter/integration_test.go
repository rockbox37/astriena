//go:build integration

package clickhouseexporter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/config/configopaque"

	"github.com/rockbox37/astriena/internal/clickhouse"
)

const integrationDB = "astriena_integration"

type integrationFixture struct {
	ctx   context.Context
	conn  driver.Conn
	ins   *chInserter
	table string
}

func integrationDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("CLICKHOUSE_DSN not set; skipping integration test (see docs/clickhouse-integration.md)")
	}
	return dsn
}

func openConn(t *testing.T, dsn string) driver.Conn {
	t.Helper()
	opts, err := clickhousego.ParseDSN(dsn)
	if err != nil {
		t.Fatal("ParseDSN: invalid DSN (see docs/clickhouse-integration.md)")
	}
	opts.Compression = &clickhousego.Compression{Method: clickhousego.CompressionZSTD}
	conn, err := clickhousego.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func uniqueTable() string {
	return fmt.Sprintf("spans_it_%d", time.Now().UnixNano())
}

func qualified(db, table string) string {
	if db == "" {
		return quoteIdent(table)
	}
	return quoteIdent(db) + "." + quoteIdent(table)
}

func ensureDatabase(t *testing.T, conn driver.Conn, db string) {
	t.Helper()
	if db == "" {
		return
	}
	ctx := context.Background()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(db)); err != nil {
		t.Fatalf("CREATE DATABASE %q: %v", db, err)
	}
}

func setupIntegration(t *testing.T) integrationFixture {
	t.Helper()
	dsn := integrationDSN(t)
	table := uniqueTable()
	conn := openConn(t, dsn)
	ensureDatabase(t, conn, integrationDB)
	t.Cleanup(func() { dropTable(t, conn, integrationDB, table) })

	cfg := &Config{
		DSN:      configopaque.String(dsn),
		Database: integrationDB,
		Table:    table,
	}
	ins, err := newInserter(cfg)
	if err != nil {
		t.Fatalf("newInserter: %v", err)
	}
	t.Cleanup(func() { _ = ins.Close() })

	return integrationFixture{
		ctx:   context.Background(),
		conn:  conn,
		ins:   ins,
		table: table,
	}
}

func dropTable(t *testing.T, conn driver.Conn, db, table string) {
	t.Helper()
	ctx := context.Background()
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+qualified(db, table)); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}
}

func waitForCount(t *testing.T, conn driver.Conn, db, table string, want uint64, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	q := fmt.Sprintf("SELECT count() FROM %s", qualified(db, table))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count uint64
		if err := conn.QueryRow(ctx, q).Scan(&count); err == nil && count >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d rows in %s", want, table)
}

func sparseColumns(t *testing.T, conn driver.Conn, db, table string) []string {
	t.Helper()
	ctx := context.Background()
	rows, err := conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database = ? AND table = ? AND startsWith(name, 'attr_') ORDER BY name",
		db, table,
	)
	if err != nil {
		t.Fatalf("query sparse columns: %v", err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan column name: %v", err)
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate columns: %v", err)
	}
	return cols
}

func attrKeys(rows []clickhouse.Row) []string {
	seen := make(map[string]bool)
	var keys []string
	for _, r := range rows {
		for k := range r.Attributes {
			if seen[k] {
				continue
			}
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys
}

func bloomIndexes(t *testing.T, conn driver.Conn, db, table string) []string {
	t.Helper()
	ctx := context.Background()
	rows, err := conn.Query(ctx,
		"SELECT name FROM system.data_skipping_indices WHERE database = ? AND table = ? AND startsWith(name, 'idx_attr_') ORDER BY name",
		db, table,
	)
	if err != nil {
		t.Fatalf("query bloom indexes: %v", err)
	}
	defer rows.Close()
	var idx []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index name: %v", err)
		}
		idx = append(idx, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate indexes: %v", err)
	}
	return idx
}

// TestDriverRealCluster exercises the clickhouse-go binding end-to-end: base
// schema, sparse columns with bloom indexes, batch insert, and query verification.
func TestDriverRealCluster(t *testing.T) {
	fix := setupIntegration(t)

	if err := fix.ins.EnsureBaseSchema(fix.ctx); err != nil {
		t.Fatalf("EnsureBaseSchema: %v", err)
	}

	rows := []clickhouse.Row{
		{
			Timestamp:  time.Date(2026, 3, 10, 12, 0, 0, 123456789, time.UTC),
			TraceID:    "trace-one",
			SpanID:     "span-one",
			Name:       "GET /health",
			StatusCode: "OK",
			DurationNS: 1_000_000,
			Attributes: map[string]string{
				"service.name": "checkout",
				"a.b":          "dot-value",
			},
		},
		{
			Timestamp:  time.Date(2026, 3, 10, 12, 0, 1, 0, time.UTC),
			TraceID:    "trace-two",
			SpanID:     "span-two",
			Name:       "POST /pay",
			StatusCode: "ERROR",
			DurationNS: 50_000_000,
			Attributes: map[string]string{
				"a-b":         "dash-value",
				"http.method": "POST",
			},
		},
	}

	if err := fix.ins.AddColumns(fix.ctx, attrKeys(rows)); err != nil {
		t.Fatalf("AddColumns: %v", err)
	}
	if err := fix.ins.InsertBatch(fix.ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	waitForCount(t, fix.conn, integrationDB, fix.table, 2, 5*time.Second)

	var name, status string
	var duration int64
	var attrService, attrDot, attrDash, attrMethod string
	q := fmt.Sprintf(`SELECT name, status_code, duration_ns,
		attributes['service.name'], attributes['a.b'], attributes['a-b'], attributes['http.method']
		FROM %s ORDER BY trace_id`, qualified(integrationDB, fix.table))
	row := fix.conn.QueryRow(fix.ctx, q)
	if err := row.Scan(&name, &status, &duration, &attrService, &attrDot, &attrDash, &attrMethod); err != nil {
		t.Fatalf("scan first row: %v", err)
	}
	if name != "GET /health" || status != "OK" || duration != 1_000_000 {
		t.Fatalf("first row mismatch: name=%q status=%q duration=%d", name, status, duration)
	}
	if attrService != "checkout" || attrDot != "dot-value" {
		t.Fatalf("first row attributes: service=%q a.b=%q", attrService, attrDot)
	}

	row = fix.conn.QueryRow(fix.ctx, q+" OFFSET 1")
	if err := row.Scan(&name, &status, &duration, &attrService, &attrDot, &attrDash, &attrMethod); err != nil {
		t.Fatalf("scan second row: %v", err)
	}
	if attrDash != "dash-value" || attrMethod != "POST" {
		t.Fatalf("second row attributes: a-b=%q http.method=%q", attrDash, attrMethod)
	}

	cols := sparseColumns(t, fix.conn, integrationDB, fix.table)
	wantCols := map[string]bool{
		attrColumn("service.name"): true,
		attrColumn("a.b"):          true,
		attrColumn("a-b"):          true,
		attrColumn("http.method"):  true,
	}
	if len(cols) != len(wantCols) {
		t.Fatalf("sparse columns = %v, want %d attr_* columns", cols, len(wantCols))
	}
	for _, c := range cols {
		if !wantCols[c] {
			t.Fatalf("unexpected sparse column %q", c)
		}
	}

	wantIdx := map[string]bool{
		"idx_" + attrColumn("service.name"): true,
		"idx_" + attrColumn("a.b"):          true,
		"idx_" + attrColumn("a-b"):          true,
		"idx_" + attrColumn("http.method"):  true,
	}
	idx := bloomIndexes(t, fix.conn, integrationDB, fix.table)
	if len(idx) != len(wantIdx) {
		t.Fatalf("bloom indexes = %v, want %d idx_attr_* entries", idx, len(wantIdx))
	}
	for _, name := range idx {
		if !wantIdx[name] {
			t.Fatalf("unexpected bloom index %q", name)
		}
	}

	colAB := attrColumn("a.b")
	colADash := attrColumn("a-b")
	sparseQ := fmt.Sprintf("SELECT `%s`, `%s` FROM %s WHERE trace_id = 'trace-one'",
		colAB, colADash, qualified(integrationDB, fix.table))
	var sparseDot, sparseDash string
	if err := fix.conn.QueryRow(fix.ctx, sparseQ).Scan(&sparseDot, &sparseDash); err != nil {
		t.Fatalf("query sparse columns: %v", err)
	}
	if sparseDot != "dot-value" {
		t.Fatalf("sparse a.b column = %q, want dot-value", sparseDot)
	}
}

// TestWriterAsyncFlush validates the Writer flush worker and time-based flush
// path against a real ClickHouse cluster.
func TestWriterAsyncFlush(t *testing.T) {
	fix := setupIntegration(t)

	w, err := clickhouse.NewWriter(clickhouse.Config{
		BatchSize:     100, // high: size flush won't trigger for a single row
		FlushInterval: 100 * time.Millisecond,
	}, fix.ins)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = w.Close(context.Background())
		}
	})

	if err := w.Start(fix.ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	row := clickhouse.Row{
		Timestamp:  time.Now().UTC(),
		TraceID:    "async-trace",
		SpanID:     "async-span",
		Name:       "async-flush",
		StatusCode: "OK",
		DurationNS: 42,
		Attributes: map[string]string{"flush.path": "interval"},
	}

	if err := w.Write(fix.ctx, []clickhouse.Row{row}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	waitForCount(t, fix.conn, integrationDB, fix.table, 1, 3*time.Second)

	if err := w.Close(fix.ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	st := w.Stats()
	if st.FlushAttempts == 0 {
		t.Fatalf("expected at least one flush attempt, stats=%+v", st)
	}

	var traceID string
	if err := fix.conn.QueryRow(fix.ctx,
		fmt.Sprintf("SELECT trace_id FROM %s WHERE trace_id = 'async-trace'", qualified(integrationDB, fix.table)),
	).Scan(&traceID); err != nil {
		t.Fatalf("query flushed row: %v", err)
	}
}
