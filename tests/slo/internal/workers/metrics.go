package workers

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"slo/internal/metrics"
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
