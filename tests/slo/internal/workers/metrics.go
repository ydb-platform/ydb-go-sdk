package workers

import (
	"context"
	"fmt"
	"sync"

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
			fmt.Printf("error while pushing: %v\n", err)
		}
	}
}
