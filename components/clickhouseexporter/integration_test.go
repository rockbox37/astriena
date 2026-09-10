//go:build integration

package clickhouseexporter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/config/configopaque"

	"github.com/rockbox37/astriena/internal/clickhouse"
)

const integrationDB = "astriena_integration"

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
		t.Fatalf("ParseDSN: %v", err)
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

// TestDriverRealCluster exercises the clickhouse-go binding end-to-end: base
// schema, sparse columns with bloom indexes, batch insert, and query verification.
func TestDriverRealCluster(t *testing.T) {
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

	ctx := context.Background()
	if err := ins.EnsureBaseSchema(ctx); err != nil {
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
				"a-b":        "dash-value",
				"http.method": "POST",
			},
		},
	}

	newKeys := []string{"service.name", "a.b", "a-b", "http.method"}
	if err := ins.AddColumns(ctx, newKeys); err != nil {
		t.Fatalf("AddColumns: %v", err)
	}
	if err := ins.InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	waitForCount(t, conn, integrationDB, table, 2, 5*time.Second)

	var name, status string
	var duration int64
	var attrService, attrDot, attrDash, attrMethod string
	q := fmt.Sprintf(`SELECT name, status_code, duration_ns,
		attributes['service.name'], attributes['a.b'], attributes['a-b'], attributes['http.method']
		FROM %s ORDER BY trace_id`, qualified(integrationDB, table))
	row := conn.QueryRow(ctx, q)
	if err := row.Scan(&name, &status, &duration, &attrService, &attrDot, &attrDash, &attrMethod); err != nil {
		t.Fatalf("scan first row: %v", err)
	}
	if name != "GET /health" || status != "OK" || duration != 1_000_000 {
		t.Fatalf("first row mismatch: name=%q status=%q duration=%d", name, status, duration)
	}
	if attrService != "checkout" || attrDot != "dot-value" {
		t.Fatalf("first row attributes: service=%q a.b=%q", attrService, attrDot)
	}

	row = conn.QueryRow(ctx, q+" OFFSET 1")
	if err := row.Scan(&name, &status, &duration, &attrService, &attrDot, &attrDash, &attrMethod); err != nil {
		t.Fatalf("scan second row: %v", err)
	}
	if attrDash != "dash-value" || attrMethod != "POST" {
		t.Fatalf("second row attributes: a-b=%q http.method=%q", attrDash, attrMethod)
	}

	cols := sparseColumns(t, conn, integrationDB, table)
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
	colAB := attrColumn("a.b")
	colADash := attrColumn("a-b")
	if colAB == colADash {
		t.Fatalf("collision keys mapped to same column: %q", colAB)
	}
	if !strings.HasPrefix(colAB, "attr_a_b_") || !strings.HasPrefix(colADash, "attr_a_b_") {
		t.Fatalf("expected attr_a_b_ prefix, got %q and %q", colAB, colADash)
	}

	// Sparse columns are DEFAULT-materialized from the attributes map.
	sparseQ := fmt.Sprintf("SELECT `%s`, `%s` FROM %s WHERE trace_id = 'trace-one'",
		colAB, colADash, qualified(integrationDB, table))
	var sparseDot, sparseDash string
	if err := conn.QueryRow(ctx, sparseQ).Scan(&sparseDot, &sparseDash); err != nil {
		t.Fatalf("query sparse columns: %v", err)
	}
	if sparseDot != "dot-value" {
		t.Fatalf("sparse a.b column = %q, want dot-value", sparseDot)
	}
}

// TestWriterAsyncFlush validates the Writer flush worker and time-based flush
// path against a real ClickHouse cluster.
func TestWriterAsyncFlush(t *testing.T) {
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

	w, err := clickhouse.NewWriter(clickhouse.Config{
		BatchSize:     100, // high: size flush won't trigger for a single row
		FlushInterval: 100 * time.Millisecond,
	}, ins)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ctx := context.Background()
	if err := w.Start(ctx); err != nil {
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

	if err := w.Write(ctx, []clickhouse.Row{row}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Write returns before insert completes; ticker should flush within interval.
	waitForCount(t, conn, integrationDB, table, 1, 3*time.Second)

	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st := w.Stats()
	if st.FlushAttempts == 0 {
		t.Fatalf("expected at least one flush attempt, stats=%+v", st)
	}

	var traceID string
	if err := conn.QueryRow(ctx,
		fmt.Sprintf("SELECT trace_id FROM %s WHERE trace_id = 'async-trace'", qualified(integrationDB, table)),
	).Scan(&traceID); err != nil {
		t.Fatalf("query flushed row: %v", err)
	}
}

