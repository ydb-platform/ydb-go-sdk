package workers

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"slo/internal/generator"
	"slo/internal/metrics"
)

type ReadWriter interface {
	Read(context.Context, generator.EntryID) (generator.Entry, error)
	Write(context.Context, generator.Entry) error
}

type Workers struct {
	ctx          context.Context
	st           ReadWriter
	m            *metrics.Metrics
	logger       *zap.Logger
	entries      generator.Entries
	entryIDs     []generator.EntryID
	entriesMutex sync.RWMutex
}

func New(ctx context.Context, st ReadWriter, m *metrics.Metrics, logger *zap.Logger) *Workers {
	w := &Workers{
		ctx:          ctx,
		st:           st,
		m:            m,
		logger:       logger,
		entries:      make(generator.Entries),
		entryIDs:     make([]generator.EntryID, 0),
		entriesMutex: sync.RWMutex{},
	}

	return w
}
