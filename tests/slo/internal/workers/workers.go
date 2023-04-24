package workers

import (
	"context"

	"go.uber.org/zap"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/metrics"
)

type ReadWriter interface {
	Read(context.Context, generator.RowID) (generator.Row, error)
	Write(context.Context, generator.Row) error
}

type Workers struct {
	cfg    *config.Config
	s      ReadWriter
	m      *metrics.Metrics
	logger *zap.Logger
}

func New(cfg *config.Config, s ReadWriter, logger *zap.Logger) (*Workers, error) {
	m, err := metrics.New(cfg.PushGateway, "native")
	if err != nil {
		logger.Error("create metrics failed", zap.Error(err))
		return nil, err
	}

	return &Workers{
		cfg:    cfg,
		s:      s,
		logger: logger,
		m:      m,
	}, nil
}

func (w *Workers) Close() error {
	return w.m.Reset()
}
