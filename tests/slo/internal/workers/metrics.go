package workers

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"slo/internal/log"
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
			log.Printf("error while pushing: %v", err)
		}
	}
}
