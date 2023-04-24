package workers

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func (w *Workers) Metrics(ctx context.Context, wg *sync.WaitGroup, rl *rate.Limiter) {
	defer wg.Done()
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		err = w.m.Push()
		if err != nil {
			w.logger.Error("error while pushing", zap.Error(err))
		}
	}
}
