package workers

import (
	"fmt"
	"math/rand"
	"reflect"

	"golang.org/x/time/rate"

	"slo/internal/metrics"
)

func (w *Workers) Read(rl *rate.Limiter) {
	for {
		err := rl.Wait(w.ctx)
		if err != nil {
			return
		}

		if len(w.entryIDs) == 0 {
			continue
		}

		i := rand.Intn(len(w.entryIDs))
		id := w.entryIDs[i]

		metricID := w.m.StartJob(metrics.JobRead)

		e, err := w.st.Read(w.ctx, id)
		if err != nil {
			w.logger.Error(fmt.Errorf("get entry error: %w", err).Error())
			w.m.StopJob(metricID, false)
			continue
		}

		w.entriesMutex.RLock()
		original := w.entries[id]
		w.entriesMutex.RUnlock()

		if reflect.DeepEqual(e, original) {
			w.m.StopJob(metricID, true)
			continue
		}
		w.logger.Info("payload does not match")
		w.m.StopJob(metricID, false)
	}
}
