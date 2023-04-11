package workers

import (
	"context"
	"fmt"
	"math/rand"

	"golang.org/x/time/rate"

	"slo/internal/metrics"
)

func (w *Workers) Read(ctx context.Context, rl *rate.Limiter) {
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		_ = w.read(ctx)
	}
}

func (w *Workers) read(ctx context.Context) (err error) {
	w.wg.Add(1)
	defer w.wg.Done()

	id := uint64(rand.Intn(int(w.cfg.InitialDataCount)))

	m := w.m.Start(metrics.JobRead)
	defer func() {
		m.Stop(err)
		w.logger.Error(fmt.Errorf("get entry error: %w", err).Error())
	}()

	_, err = w.st.Read(ctx, id)

	// todo: check

	return err
}
