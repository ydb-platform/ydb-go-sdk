package workers

import (
	"context"
	"fmt"
	"time"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/metrics"
)

type ReadWriter interface {
	Read(ctx context.Context, rowID generator.RowID) (_ generator.Row, attempts int, err error)
	Write(ctx context.Context, row generator.Row) (attempts int, err error)
}

type Workers struct {
	cfg *config.Config
	s   ReadWriter
	m   *metrics.Metrics
}

func New(cfg *config.Config, s ReadWriter, label, jobName string) (*Workers, error) {
	m, err := metrics.New(cfg.PushGateway, label, jobName)
	if err != nil {
		fmt.Printf("[%s] create metrics failed: %v\n", time.Now().Format(time.RFC3339), err)

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
