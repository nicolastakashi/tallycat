package otelprocessortests

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

type Config struct {
	// FlushInterval is the interval at which schemas are emitted as logs
	FlushInterval time.Duration `mapstructure:"flush_interval"`
}

func (cfg *Config) Validate() error {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 1 * time.Minute // Default to 1 minute
	}
	return nil
}
