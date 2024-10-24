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
		select {
		case <-ctx.Done():
			return
		default:
			err := rl.Wait(ctx)
			if err != nil {
				return
			}

			err = w.write(ctx, gen)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr == nil {
					log.Printf("write failed: %v", err)
				}
			}
		}
	}
}

func (w *Workers) write(ctx context.Context, gen *generator.Generator) error {
	row, err := gen.Generate()
	if err != nil {
		log.Printf("generate error: %v", err)

		return err
	}

	m := w.m.Start(metrics.OperationTypeWrite)

	attempts, err := w.s.Write(ctx, row)

	m.Finish(err, attempts)

	return err
}
