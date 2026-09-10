package astrienasampler

import (
	"fmt"
	"time"
)

// Config is the Collector configuration for the Astriena tail-sampling processor.
// It maps directly onto sampling.Config plus a serializable policy list.
//
// An empty Policies list is valid: no policy ever keeps a trace, so every
// buffered id is default-dropped when DecisionWait elapses. That is the
// keep-nothing / default-drop-only configuration.
type Config struct {
	DecisionWait     time.Duration `mapstructure:"decision_wait"`
	MaxTraces        int           `mapstructure:"max_traces"`
	MaxSpansPerTrace int           `mapstructure:"max_spans_per_trace"`
	Policies         []PolicyCfg   `mapstructure:"policies"`
}

// PolicyCfg is one keep/drop rule in config form. The processor translates these
// into sampling.Policy implementations.
type PolicyCfg struct {
	Type        string `mapstructure:"type"`         // status_code | latency
	Keep        string `mapstructure:"keep"`         // for status_code, e.g. "ERROR"
	ThresholdMS int64  `mapstructure:"threshold_ms"` // for latency; must be > 0
}

// Validate checks policy types and required fields. Collector component
// validation and `astriena validate --config` both call this, same as the
// ClickHouse exporter. Empty Policies is allowed (default-drop only).
func (c *Config) Validate() error {
	for i, p := range c.Policies {
		switch p.Type {
		case "status_code":
			if p.Keep == "" {
				return fmt.Errorf("astriena_sampler: policies[%d]: status_code requires keep", i)
			}
		case "latency":
			if p.ThresholdMS <= 0 {
				return fmt.Errorf("astriena_sampler: policies[%d]: latency requires threshold_ms > 0", i)
			}
		case "":
			return fmt.Errorf("astriena_sampler: policies[%d]: type is required", i)
		default:
			return fmt.Errorf("astriena_sampler: policies[%d]: unknown type %q (want status_code or latency)", i, p.Type)
		}
	}
	return nil
}
