package workers

import (
	"context"

	"go.uber.org/zap"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/metrics"
)

type ReadWriter interface {
	Read(context.Context, generator.RowID) (_ generator.Row, attempts int, err error)
	Write(context.Context, generator.Row) (attempts int, err error)
}

type Workers struct {
	cfg    *config.Config
	s      ReadWriter
	m      *metrics.Metrics
	logger *zap.Logger
}

func New(cfg *config.Config, s ReadWriter, logger *zap.Logger, label, jobName string) (*Workers, error) {
	logger = logger.Named("workers")

	m, err := metrics.New(logger, cfg.PushGateway, label, jobName)
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
