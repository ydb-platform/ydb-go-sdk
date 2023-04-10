package workers

import (
	"fmt"

	"golang.org/x/time/rate"
)

func (w *Workers) Metrics(rl *rate.Limiter) {
	for {
		err := rl.Wait(w.shutdownCtx)
		if err != nil {
			return
		}

		err = w.m.Push()
		if err != nil {
			w.logger.Error(fmt.Errorf("error while pushing: %w", err).Error())
		}
	}
}
