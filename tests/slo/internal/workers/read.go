package workers

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"golang.org/x/time/rate"

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

func (w *Workers) read(ctx context.Context) (err error) {
	id := uint64(rand.Intn(int(w.cfg.InitialDataCount))) //nolint:gosec // speed more important

	var attempts int

	m := w.m.Start(metrics.JobRead)
	defer func() {
		m.Stop(err, attempts)
		if err != nil {
			fmt.Printf("get entry error: %v\n", err)
		}
	}()

	_, attempts, err = w.s.Read(ctx, id)

	return err
}
