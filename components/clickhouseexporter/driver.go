package clickhouseexporter

import (
	"context"
	"fmt"
	"strings"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/rockbox37/astriena/internal/clickhouse"
)

// chInserter implements clickhouse.Inserter against a real ClickHouse cluster
// using the official clickhouse-go/v2 driver. It is the only place in Astriena
// that imports the driver; the pure internal/clickhouse package owns the
// batching and auto-schema logic and calls through this port.
type chInserter struct {
	conn     driver.Conn
	database string
	table    string
}

// newInserter opens a (lazily connected) driver.Conn from the BYOS DSN and
// applies ZSTD compression for the batched writes. Database/table default from
// the exporter config.
func newInserter(cfg *Config) (*chInserter, error) {
	opts, err := clickhousego.ParseDSN(string(cfg.DSN))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse dsn: %w", err)
	}
	database := cfg.Database
	if database == "" {
		database = opts.Auth.Database
	}
	if database != "" {
		opts.Auth.Database = database
	}
	// Compress the wire batches; the customer pays for their own storage/egress.
	opts.Compression = &clickhousego.Compression{Method: clickhousego.CompressionZSTD}

	conn, err := clickhousego.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	return &chInserter{conn: conn, database: database, table: cfg.Table}, nil
}

// qualified returns `db`.`table`, or just `table` when no database is set.
func (c *chInserter) qualified() string {
	if c.database == "" {
		return quoteIdent(c.table)
	}
	return quoteIdent(c.database) + "." + quoteIdent(c.table)
}

// EnsureBaseSchema creates the database and the spans table (fixed columns) if
// they do not already exist. Per-column ZSTD codecs keep the customer's storage
// footprint down; the ordering key matches the common time-range + trace lookup.
func (c *chInserter) EnsureBaseSchema(ctx context.Context) error {
	if c.database != "" {
		if err := c.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(c.database)); err != nil {
			return fmt.Errorf("clickhouse: create database: %w", err)
		}
	}
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  timestamp    DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
  trace_id     String CODEC(ZSTD(1)),
  span_id      String CODEC(ZSTD(1)),
  name         LowCardinality(String) CODEC(ZSTD(1)),
  status_code  LowCardinality(String) CODEC(ZSTD(1)),
  duration_ns  Int64 CODEC(ZSTD(1)),
  attributes   Map(String, String) CODEC(ZSTD(1))
) ENGINE = MergeTree
ORDER BY (timestamp, trace_id)`, c.qualified())
	if err := c.conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("clickhouse: create table: %w", err)
	}
	return nil
}

// AddColumns materializes a sparse String column for each new attribute key,
// defaulted from the attributes map, plus a bloom-filter data-skipping index so
// high-cardinality lookups stay fast. All statements are IF NOT EXISTS, so this
// is safe to call repeatedly and from concurrent flushes.
//
// TODO(core): two distinct keys can sanitize to the same column name (e.g.
// "a.b" and "a-b"); disambiguate (hash suffix) before this is load-bearing.
func (c *chInserter) AddColumns(ctx context.Context, keys []string) error {
	tbl := c.qualified()
	for _, k := range keys {
		col := attrColumn(k)
		addCol := fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s String DEFAULT attributes[%s] CODEC(ZSTD(1))",
			tbl, quoteIdent(col), quoteString(k),
		)
		if err := c.conn.Exec(ctx, addCol); err != nil {
			return fmt.Errorf("clickhouse: add column %q: %w", col, err)
		}
		addIdx := fmt.Sprintf(
			"ALTER TABLE %s ADD INDEX IF NOT EXISTS %s %s TYPE bloom_filter GRANULARITY 4",
			tbl, quoteIdent("idx_"+col), quoteIdent(col),
		)
		if err := c.conn.Exec(ctx, addIdx); err != nil {
			return fmt.Errorf("clickhouse: add index for %q: %w", col, err)
		}
	}
	return nil
}

// InsertBatch writes rows via a prepared batch over the fixed column set. The
// sparse attribute columns are DEFAULT-materialized from the attributes map, so
// they never appear in the INSERT — the statement stays stable as the schema grows.
func (c *chInserter) InsertBatch(ctx context.Context, rows []clickhouse.Row) error {
	stmt := fmt.Sprintf(
		"INSERT INTO %s (timestamp, trace_id, span_id, name, status_code, duration_ns, attributes)",
		c.qualified(),
	)
	batch, err := c.conn.PrepareBatch(ctx, stmt)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare batch: %w", err)
	}
	for _, r := range rows {
		attrs := r.Attributes
		if attrs == nil {
			attrs = map[string]string{}
		}
		if err := batch.Append(
			r.Timestamp, r.TraceID, r.SpanID, r.Name, r.StatusCode, r.DurationNS, attrs,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("clickhouse: append row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send batch: %w", err)
	}
	return nil
}

func (c *chInserter) Close() error { return c.conn.Close() }

// attrColumn maps an attribute key to its sparse column name, replacing any
// character that is not a letter, digit, or underscore so the identifier is
// valid. e.g. "http.method" -> "attr_http_method".
func attrColumn(key string) string {
	var b strings.Builder
	b.WriteString("attr_")
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// quoteIdent wraps a ClickHouse identifier in backticks, escaping any backtick.
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// quoteString renders a ClickHouse string literal, escaping single quotes.
func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "\\'") + "'"
}
