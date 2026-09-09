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
