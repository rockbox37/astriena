package clickhouseexporter

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
)

// Config is the Collector configuration for the ClickHouse exporter. It maps
// directly onto clickhouse.Config. The DSN points at the customer's own cluster
// (BYOS) and is typically supplied via an env var.
//
// DSN is configopaque.String so the embedded credential is redacted whenever the
// effective configuration is marshaled (config dumps, zpages, validate output,
// errors that echo config) rather than printed in cleartext.
type Config struct {
	DSN           configopaque.String `mapstructure:"dsn"`
	Database      string              `mapstructure:"database"`
	Table         string              `mapstructure:"table"`
	BatchSize     int                 `mapstructure:"batch_size"`
	FlushInterval time.Duration       `mapstructure:"flush_interval"`
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.DSN == "" {
		return errors.New("clickhouse: dsn is required (BYOS: point it at your own cluster)")
	}
	if c.BatchSize < 0 {
		return errors.New("clickhouse: batch_size must not be negative")
	}
	if c.FlushInterval < 0 {
		return errors.New("clickhouse: flush_interval must not be negative")
	}
	return nil
}
