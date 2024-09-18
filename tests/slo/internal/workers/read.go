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
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		_ = w.read(ctx)
	}
}

func (w *Workers) read(ctx context.Context) error {
	id := uint64(rand.Intn(int(w.cfg.InitialDataCount))) //nolint:gosec // speed more important

	m := w.m.Start(metrics.JobRead)

	_, attempts, err := w.s.Read(ctx, id)
	if err != nil {
		log.Printf("read failed with %d attempts: %v", attempts, err)
	}

	m.Finish(err, attempts)

	return err
}
