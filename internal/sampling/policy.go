package sampling

import "strings"

// StatusCodePolicy keeps any trace that contains a span with a matching status
// (e.g. keep 100% of traces with an ERROR span).
type StatusCodePolicy struct {
	Keep string // e.g. "ERROR"
}

func (p StatusCodePolicy) Name() string { return "status_code" }

func (p StatusCodePolicy) Evaluate(t *Trace) Decision {
	for _, s := range t.Spans {
		if strings.EqualFold(s.StatusCode, p.Keep) {
			return DecisionSampled
		}
	}
	return DecisionPending
}

// LatencyPolicy keeps traces containing any span at or above ThresholdNS.
type LatencyPolicy struct {
	ThresholdNS int64
}

func (p LatencyPolicy) Name() string { return "latency" }

func (p LatencyPolicy) Evaluate(t *Trace) Decision {
	for _, s := range t.Spans {
		if s.DurationNS >= p.ThresholdNS {
			return DecisionSampled
		}
	}
	return DecisionPending
}
