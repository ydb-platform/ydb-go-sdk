package workers

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/time/rate"

	"slo/internal/generator"
	"slo/internal/metrics"
)

func (w *Workers) Write(ctx context.Context, wg *sync.WaitGroup, rl *rate.Limiter, gen *generator.Generator) {
	defer wg.Done()
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return
		}

		_ = w.write(ctx, gen)
	}
}

func (w *Workers) write(ctx context.Context, gen *generator.Generator) (err error) {
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

	return w.s.Write(ctx, row)
}
