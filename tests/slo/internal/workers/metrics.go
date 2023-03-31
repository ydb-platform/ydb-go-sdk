package workers

import (
	"fmt"

	"slo/internal/metrics"

	"github.com/beefsack/go-rate"
	"go.uber.org/zap"
)

func Metrics(rl *rate.RateLimiter, m *metrics.Metrics, logger *zap.Logger) {
	for {
		rl.Wait()
		err := m.Push()
		if err != nil {
			logger.Error(fmt.Errorf("error while pushing: %w", err).Error())
		}
	}
}
