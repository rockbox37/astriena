package astrienasampler

import "time"

// Config is the Collector configuration for the Astriena tail-sampling processor.
// It maps directly onto sampling.Config plus a serializable policy list.
type Config struct {
	DecisionWait time.Duration `mapstructure:"decision_wait"`
	MaxTraces    int           `mapstructure:"max_traces"`
	Policies     []PolicyCfg   `mapstructure:"policies"`
}

// PolicyCfg is one keep/drop rule in config form. The processor translates these
// into sampling.Policy implementations.
type PolicyCfg struct {
	Type        string `mapstructure:"type"`         // status_code | latency | ...
	Keep        string `mapstructure:"keep"`         // for status_code, e.g. "ERROR"
	ThresholdMS int64  `mapstructure:"threshold_ms"` // for latency
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	// TODO(core): validate policy types and required fields.
	return nil
}
