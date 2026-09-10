package clickhouseexporter

import (
	"strings"
	"testing"
)

func TestQuoteStringEscapesBackslashAndQuote(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`plain`, `'plain'`},
		{`o'reilly`, `'o\'reilly'`},
		{`a\b`, `'a\\b'`},
		{`x\x27], DROP COLUMN timestamp --`, `'x\\x27], DROP COLUMN timestamp --'`},
	}
	for _, tc := range cases {
		if got := quoteString(tc.in); got != tc.want {
			t.Errorf("quoteString(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestNewInserterInvalidDSNDoesNotEchoSecret(t *testing.T) {
	_, err := newInserter(&Config{DSN: "clickhouse://user:s3cret%zz@host:9000/db"})
	if err == nil {
		t.Fatal("expected invalid DSN to fail")
	}
	msg := err.Error()
	if strings.Contains(msg, "s3cret") || strings.Contains(msg, "user") || strings.Contains(msg, "%zz") {
		t.Fatalf("ParseDSN error leaked credential material: %q", msg)
	}
}

func TestAttrColumnDisambiguatesSanitizedCollisions(t *testing.T) {
	a := attrColumn("a.b")
	b := attrColumn("a-b")
	if a == b {
		t.Fatalf("colliding keys mapped to same column: %q", a)
	}
	if !strings.HasPrefix(a, "attr_a_b_") || !strings.HasPrefix(b, "attr_a_b_") {
		t.Fatalf("expected shared sanitized prefix attr_a_b_, got %q and %q", a, b)
	}
}

func TestAttrColumnStableAcrossCalls(t *testing.T) {
	key := "service.name"
	first := attrColumn(key)
	for i := 0; i < 10; i++ {
		if got := attrColumn(key); got != first {
			t.Fatalf("attrColumn(%q) unstable: first %q, got %q", key, first, got)
		}
	}
}

func TestAttrColumnSimpleKeyRegression(t *testing.T) {
	col := attrColumn("http.method")
	wantPrefix := "attr_http_method_"
	if !strings.HasPrefix(col, wantPrefix) {
		t.Fatalf("attrColumn(http.method) = %q, want prefix %q", col, wantPrefix)
	}
	suffix := strings.TrimPrefix(col, wantPrefix)
	if len(suffix) != 8 {
		t.Fatalf("hash suffix = %q (len %d), want 8 hex chars", suffix, len(suffix))
	}
	for _, r := range suffix {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			t.Fatalf("hash suffix %q is not lowercase hex", suffix)
		}
	}
}

func TestAttrColumnDistinctKeysStayDistinct(t *testing.T) {
	keys := []string{"a.b", "a-b", "a_b", "http.method", "http-method", "x/y", "x_y"}
	seen := make(map[string]string, len(keys))
	for _, k := range keys {
		col := attrColumn(k)
		if prev, ok := seen[col]; ok {
			t.Fatalf("keys %q and %q both map to %q", prev, k, col)
		}
		seen[col] = k
	}
}
