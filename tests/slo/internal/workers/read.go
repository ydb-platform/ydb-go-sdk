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

		id := uint64(rand.Intn(int(w.cfg.InitialDataCount)))

		metricID := w.m.StartJob(metrics.JobRead)

		_, err = w.st.Read(ctx, id)
		if err != nil {
			w.logger.Error(fmt.Errorf("get entry error: %w", err).Error())
			w.m.StopJob(metricID, false)
			continue
		}

		// todo: check

		w.m.StopJob(metricID, true)
	}
}
