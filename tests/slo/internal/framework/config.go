package framework

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Endpoint string
	Database string

	Ref   string
	Label string

	Duration int

	OTLPEndpoint       string
	PrometheusEndpoint string
}

func NewConfig() (*Config, error) {
	cfg := &Config{}

	cfg.Endpoint = os.Getenv("YDB_ENDPOINT")
	cfg.Database = os.Getenv("YDB_DATABASE")

	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("YDB_ENDPOINT is required")
	}

	if cfg.Database == "" {
		return nil, fmt.Errorf("YDB_DATABASE is required")
	}

	cfg.Ref = os.Getenv("WORKLOAD_REF")
	cfg.Label = os.Getenv("WORKLOAD_NAME")

	cfg.Duration = envDefaultInt("WORKLOAD_DURATION", 600) //nolint:mnd

	cfg.OTLPEndpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	cfg.PrometheusEndpoint = os.Getenv("PROMETHEUS_URL")

	if cfg.Duration <= 0 {
		return nil, fmt.Errorf("WORKLOAD_DURATION must be > 0, got %d", cfg.Duration)
	}

	return cfg, nil
}

func (c *Config) RunDuration() time.Duration {
	return time.Duration(c.Duration) * time.Second
}

func envDefaultInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}

	return defaultVal
}
