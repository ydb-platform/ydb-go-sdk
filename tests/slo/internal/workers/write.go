package workers

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"slo/internal/generator"
	"slo/internal/log"
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

func (w *Workers) write(ctx context.Context, gen *generator.Generator) error {
	row, err := gen.Generate()
	if err != nil {
		log.Printf("generate error: %v", err)

		return err
	}

	m := w.m.Start(metrics.JobWrite)

	attempts, err := w.s.Write(ctx, row)
	if err != nil {
		log.Printf("write failed with %d attempts: %v", attempts, err)
	}

	m.Finish(err, attempts)

	return err
}
