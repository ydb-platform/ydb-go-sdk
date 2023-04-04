package workers

import (
	"context"
	"fmt"
	"sync"

	"slo/internal/generator"
	"slo/internal/metrics"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func Write(ctx context.Context, st Storager, rl *rate.Limiter, m *metrics.Metrics, logger *zap.Logger,
	gen generator.Generator, en generator.Entries, ids *[]generator.EntryID, mu *sync.RWMutex,
) {
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		entry, err := gen.Generate()
		if err != nil {
			logger.Error(fmt.Errorf("generate error: %w", err).Error())
			continue
		}

		metricID := m.StartJob(metrics.JobWrite)

		err = st.Write(ctx, entry)
		if err != nil {
			logger.Error(fmt.Errorf("error when write entry: %w", err).Error())
			m.StopJob(metricID, false)
			continue
		}

		m.StopJob(metricID, true)

		mu.Lock()
		en[entry.ID] = entry
		*ids = append(*ids, entry.ID)
		mu.Unlock()
	}
}
