package workers

import (
	"fmt"
	"go.uber.org/zap"
	"slo/internal/metrics"

	"github.com/beefsack/go-rate"
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
