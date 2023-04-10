package workers

import (
	"context"

	"go.uber.org/zap"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/metrics"
)

type ReadWriter interface {
	Read(context.Context, generator.EntryID) (generator.Entry, error)
	Write(context.Context, generator.Entry) error
}

type Workers struct {
	cfg    *config.Config
	st     ReadWriter
	m      *metrics.Metrics
	logger *zap.Logger
}

func New(cfg *config.Config, st ReadWriter, m *metrics.Metrics, logger *zap.Logger) *Workers {
	return &Workers{
		cfg:    cfg,
		st:     st,
		m:      m,
		logger: logger,
	}
}
