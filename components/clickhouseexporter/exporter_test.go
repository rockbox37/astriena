package clickhouseexporter

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestToRows(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	sp := ss.Spans().AppendEmpty()
	sp.SetTraceID(pcommon.TraceID([16]byte{0x01}))
	sp.SetSpanID(pcommon.SpanID([8]byte{0x02}))
	sp.SetName("GET /checkout")
	sp.Status().SetCode(ptrace.StatusCodeError)
	sp.SetStartTimestamp(0)
	sp.SetEndTimestamp(pcommon.Timestamp(200 * time.Millisecond))
	sp.Attributes().PutStr("user_tier", "premium")

	rows := toRows(td)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r.Name != "GET /checkout" {
		t.Errorf("Name = %q, want GET /checkout", r.Name)
	}
	if r.StatusCode != "ERROR" {
		t.Errorf("StatusCode = %q, want ERROR", r.StatusCode)
	}
	if r.DurationNS != int64(200*time.Millisecond) {
		t.Errorf("DurationNS = %d, want %d", r.DurationNS, int64(200*time.Millisecond))
	}
	if r.Attributes["user_tier"] != "premium" {
		t.Errorf("attribute not flattened: %+v", r.Attributes)
	}
	if r.TraceID == "" {
		t.Errorf("TraceID not populated")
	}
}
