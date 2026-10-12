package framework

import (
	"context"
	"errors"
	"os"

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
					if err := rl.Wait(workerCtx); err != nil {
						return nil
					}
					if err := fn(workerCtx); err != nil {
						if workerCtx.Err() != nil && errors.Is(err, context.Canceled) {
							return nil
						}

						return err
					}
					if os.Getenv("SLO_QA_FINAL_ERROR") == "1" {
						return errors.New("slo qa final operation: SLO_QA_GO_FINAL_OPERATION")
					}
				}
			}
		})
	}

	return group.Wait()
}
