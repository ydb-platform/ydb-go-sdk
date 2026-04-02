package framework

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

func NewRateLimiter(rps int) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(rps), 1)
}

func RunWorkers(
	ctx context.Context,
	n int,
	rl *rate.Limiter,
	fn func(ctx context.Context),
) {
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := rl.Wait(ctx); err != nil {
						return
					}
					fn(ctx)
				}
			}
		}()
	}
	wg.Wait()
}
