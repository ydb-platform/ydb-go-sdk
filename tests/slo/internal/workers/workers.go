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
	ctx    context.Context
	cfg    config.Config
	st     ReadWriter
	m      *metrics.Metrics
	logger *zap.Logger
}

func New(ctx context.Context, cfg config.Config, st ReadWriter, m *metrics.Metrics, logger *zap.Logger) *Workers {
	w := &Workers{
		ctx:    ctx,
		cfg:    cfg,
		st:     st,
		m:      m,
		logger: logger,
	}

	return w
}
