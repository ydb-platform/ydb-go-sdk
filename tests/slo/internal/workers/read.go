package workers

import (
	"context"
	"math/rand"
	"sync"

	"golang.org/x/time/rate"

	"slo/internal/log"
	"slo/internal/metrics"
)

func (w *Workers) Read(ctx context.Context, wg *sync.WaitGroup, rl *rate.Limiter) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := rl.Wait(ctx)
			if err != nil {
				return
			}

			err = w.read(ctx)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr == nil {
					log.Printf("read failed: %v", err)
				}

				return
			}
		}
	}
}

func (w *Workers) read(ctx context.Context) error {
	id := uint64(rand.Intn(int(w.cfg.InitialDataCount))) //nolint:gosec // speed more important

	m := w.m.Start(metrics.OperationTypeRead)

	_, attempts, err := w.s.Read(ctx, id)

	m.Finish(err, attempts)

	return err
}
