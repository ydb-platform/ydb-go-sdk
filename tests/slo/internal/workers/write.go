package workers

import (
	"fmt"

	"golang.org/x/time/rate"

	"slo/internal/generator"
	"slo/internal/metrics"
)

func (w *Workers) Write(rl *rate.Limiter, gen generator.Generator) {
	for {
		err := rl.Wait(w.shutdownCtx)
		if err != nil {
			return
		}

		entry, err := gen.Generate()
		if err != nil {
			w.logger.Error(fmt.Errorf("generate error: %w", err).Error())
			continue
		}

		metricID := w.m.StartJob(metrics.JobWrite)

		err = w.st.Write(w.ctx, entry)
		if err != nil {
			w.logger.Error(fmt.Errorf("error when write entry: %w", err).Error())
			w.m.StopJob(metricID, false)
			continue
		}

		w.m.StopJob(metricID, true)
	}
}
