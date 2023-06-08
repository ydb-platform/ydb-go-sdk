package workers

import (
	"context"
	"sync"

	"go.uber.org/zap"
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
		w.logger.Error("generate error", zap.Error(err))
		return err
	}

	var attempts int

	m := w.m.Start(metrics.JobWrite)
	defer func() {
		m.Stop(err, attempts)
		if err != nil {
			w.logger.Error("error when stop 'write' worker", zap.Error(err))
		}
	}()

	attempts, err = w.s.Write(ctx, row)

	return err
}
