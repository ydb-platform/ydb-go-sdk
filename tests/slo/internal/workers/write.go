package workers

import (
	"context"
	"fmt"

	"golang.org/x/time/rate"

	"slo/internal/generator"
	"slo/internal/metrics"
)

func (w *Workers) Write(ctx context.Context, rl *rate.Limiter, gen *generator.Generator) {
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		_ = w.write(ctx, gen)
	}
}

func (w *Workers) write(ctx context.Context, gen *generator.Generator) (err error) {
	w.wg.Add(1)
	defer w.wg.Done()

	var row generator.Row
	row, err = gen.Generate()
	if err != nil {
		w.logger.Error(fmt.Errorf("generate error: %w", err).Error())
		return err
	}

	m := w.m.Start(metrics.JobWrite)
	defer func() {
		m.Stop(err)
		w.logger.Error(fmt.Errorf("error when 'write' entry: %w", err).Error())
	}()

	return w.st.Write(ctx, row)
}
