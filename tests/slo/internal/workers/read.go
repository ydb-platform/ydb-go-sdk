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
			}
		}
	}
}

func (w *Workers) read(ctx context.Context) error {
	var m metrics.Span
	var attempts int
	var err error
	if w.s != nil {
		id := uint64(rand.Intn(int(w.cfg.InitialDataCount))) //nolint:gosec // speed more important
		m = w.m.Start(metrics.OperationTypeRead)
		_, attempts, err = w.s.Read(ctx, id)
	} else {
		ids := make([]uint64, 0, w.cfg.BatchSize)
		for range w.cfg.BatchSize {
			ids = append(ids, uint64(rand.Intn(int(w.cfg.InitialDataCount)))) //nolint:gosec
		}
		m = w.m.Start(metrics.OperationTypeRead)
		_, attempts, err = w.sb.ReadBatch(ctx, ids)
	}

	m.Finish(err, attempts)

	return err
}
