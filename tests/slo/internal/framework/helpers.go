package framework

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

func NewRateLimiter(rps int) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(rps), 1)
}

func RunWorkers(
	ctx context.Context,
	n int,
	rl *rate.Limiter,
	fn func(ctx context.Context) error,
) error {
	group, workerCtx := errgroup.WithContext(ctx)
	for range n {
		group.Go(func() error {
			for {
				select {
				case <-workerCtx.Done():
					return nil
				default:
					err := rl.Wait(workerCtx)
					if err != nil && errors.Is(err, workerCtx.Err()) {
						return nil
					}
					if err != nil {
						return err
					}
					if err := fn(workerCtx); err != nil {
						if workerCtx.Err() != nil && errors.Is(err, context.Canceled) {
							return nil
						}

						return err
					}
				}
			}
		})
	}

	return group.Wait()
}
