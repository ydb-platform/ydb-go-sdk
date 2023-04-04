package workers

import (
	"context"
	"fmt"

	"slo/internal/metrics"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func Metrics(ctx context.Context, rl *rate.Limiter, m *metrics.Metrics, logger *zap.Logger) {
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		err = m.Push()
		if err != nil {
			logger.Error(fmt.Errorf("error while pushing: %w", err).Error())
		}
	}
}
