package workers

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"math/rand"
	"reflect"
	"sync"

	"slo/internal/generator"
	"slo/internal/metrics"

	"github.com/beefsack/go-rate"
)

func Read(st Storager, rl *rate.RateLimiter, m *metrics.Metrics, logger *zap.Logger,
	en generator.Entries, idsPtr *[]generator.EntryID, mu *sync.RWMutex, endChan chan struct{},
) {
	for {
		select {
		case <-endChan:
			return
		default:
		}

		ids := *idsPtr

		if len(ids) == 0 {
			continue
		}

		i := rand.Intn(len(ids))
		id := ids[i]

		rl.Wait()

		metricID := m.StartJob(metrics.JobRead)

		e, err := st.Read(context.Background(), id)
		if err != nil {
			logger.Error(fmt.Errorf("get entry error: %w", err).Error())
			m.StopJob(metricID, false)
			continue
		}

		mu.RLock()
		original := en[id]
		mu.RUnlock()

		if reflect.DeepEqual(e, original) {
			m.StopJob(metricID, true)
			continue
		}
		logger.Info("payload does not match")
		m.StopJob(metricID, false)
	}
}
