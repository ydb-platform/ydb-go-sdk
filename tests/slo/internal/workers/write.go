package workers

import (
	"context"
	"fmt"
	"sync"

	"slo/internal/generator"
	"slo/internal/metrics"

	"github.com/beefsack/go-rate"
	"go.uber.org/zap"
)

func Write(st Storager, rl *rate.RateLimiter, m *metrics.Metrics, logger *zap.Logger,
	gen generator.Generator, en generator.Entries, ids *[]generator.EntryID, mu *sync.RWMutex, endChan chan struct{},
) {
	for {
		select {
		case <-endChan:
			return
		default:
		}

		entry, err := gen.Generate()
		if err != nil {
			logger.Error(fmt.Errorf("generate error: %w", err).Error())
			continue
		}

		rl.Wait()

		metricID := m.StartJob(metrics.JobWrite)

		err = st.Write(context.Background(), entry)
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
