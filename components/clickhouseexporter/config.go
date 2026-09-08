package clickhouseexporter

import "time"

// Config is the Collector configuration for the ClickHouse exporter. It maps
// directly onto clickhouse.Config. The DSN points at the customer's own cluster
// (BYOS) and is typically supplied via an env var.
type Config struct {
	DSN           string        `mapstructure:"dsn"`
	Database      string        `mapstructure:"database"`
	Table         string        `mapstructure:"table"`
	BatchSize     int           `mapstructure:"batch_size"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	// TODO(core): require DSN, sanity-check batch settings.
	return nil
}
