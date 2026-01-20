package workers

import (
	"context"
	"time"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/log"
	"slo/internal/metrics"
)

type ReadWriter interface {
	Read(ctx context.Context, rowID generator.RowID) (_ generator.Row, attempts int, err error)
	Write(ctx context.Context, row generator.Row) (attempts int, err error)
}

type ReadWriterNodeHints interface {
	Read(ctx context.Context, rowID generator.RowID) (_ generator.Row, attempts int, missed bool, err error)
	Write(ctx context.Context, row generator.Row) (attempts int, missed bool, err error)
}

type BatchReadWriter interface {
	ReadBatch(ctx context.Context, rowIDs []generator.RowID) (_ []generator.Row, attempts int, err error)
	WriteBatch(ctx context.Context, rows []generator.Row) (attempts int, err error)
}

type MissesWrapper struct {
	readWriter ReadWriter
}

func (w *MissesWrapper) Read(ctx context.Context, rowID generator.RowID) (generator.Row, int, bool, error) {
	row, attempts, err := w.readWriter.Read(ctx, rowID)
	return row, attempts, false, err
}
func (w *MissesWrapper) Write(ctx context.Context, row generator.Row) (int, bool, error) {
	attempts, err := w.readWriter.Write(ctx, row)
	return attempts, false, err
}

func wrapWithMisses(s ReadWriter) ReadWriterNodeHints {
	return &MissesWrapper{readWriter: s}
}

type Workers struct {
	cfg *config.Config
	s   ReadWriterNodeHints
	sb  BatchReadWriter
	m   *metrics.Metrics
	Gen generator.Generator
}

func New(cfg *config.Config, s ReadWriter, ref, label, jobName string) (*Workers, error) {
	m, err := metrics.New(cfg.OTLPEndpoint, ref, label, jobName, cfg.ReportPeriod)
	if err != nil {
		log.Printf("create metrics failed: %v", err)

		return nil, err
	}

	return &Workers{
		cfg: cfg,
		s:   wrapWithMisses(s),
		m:   m,
	}, nil
}

func NewWithNodeHints(cfg *config.Config, s ReadWriterNodeHints, ref, label, jobName string) (*Workers, error) {
	m, err := metrics.New(cfg.OTLPEndpoint, ref, label, jobName, cfg.ReportPeriod)
	if err != nil {
		log.Printf("create metrics failed: %v", err)

		return nil, err
	}

	return &Workers{
		cfg: cfg,
		s:   s,
		m:   m,
	}, nil
}

func NewWithBatch(cfg *config.Config, s BatchReadWriter, ref, label, jobName string) (*Workers, error) {
	m, err := metrics.New(cfg.OTLPEndpoint, ref, label, jobName, cfg.ReportPeriod)
	if err != nil {
		log.Printf("create metrics failed: %v", err)

		return nil, err
	}

	return &Workers{
		cfg: cfg,
		sb:  s,
		m:   m,
	}, nil
}

func (w *Workers) ReportNodeHintMisses(val int64) {
	if w.m != nil {
		w.m.ReportNodeHintMisses(val)
	}
}

func (w *Workers) ExportInterval() time.Duration {
	return w.m.ExportInterval
}

func (w *Workers) Close() error {
	if w.m != nil {
		return w.m.Close()
	}

	return nil
}
