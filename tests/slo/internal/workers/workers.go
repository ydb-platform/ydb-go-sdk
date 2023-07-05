package workers

import (
	"context"
	"fmt"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/metrics"
)

type ReadWriter interface {
	Read(context.Context, generator.RowID) (_ generator.Row, attempts int, err error)
	Write(context.Context, generator.Row) (attempts int, err error)
}

type Workers struct {
	cfg *config.Config
	s   ReadWriter
	m   *metrics.Metrics
}

func New(cfg *config.Config, s ReadWriter, label, jobName string) (*Workers, error) {
	m, err := metrics.New(cfg.PushGateway, label, jobName)
	if err != nil {
		fmt.Printf("create metrics failed: %v\n", err)
		return nil, err
	}

	return &Workers{
		cfg: cfg,
		s:   s,
		m:   m,
	}, nil
}

func (w *Workers) Close() error {
	return w.m.Reset()
}
