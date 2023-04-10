package workers

import (
	"context"
	"fmt"

	"golang.org/x/time/rate"
)

func (w *Workers) Metrics(ctx context.Context, rl *rate.Limiter) {
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		err = w.m.Push()
		if err != nil {
			w.logger.Error(fmt.Errorf("error while pushing: %w", err).Error())
		}
	}
}
