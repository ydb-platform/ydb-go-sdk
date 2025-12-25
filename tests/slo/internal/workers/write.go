package workers

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"slo/internal/generator"
	"slo/internal/log"
	"slo/internal/metrics"
)

func (w *Workers) Write(ctx context.Context, wg *sync.WaitGroup, rl *rate.Limiter, gen generator.Generator) {
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

func (w *Workers) write(ctx context.Context, gen generator.Generator) (finalErr error) {
	m := w.m.Start(metrics.OperationTypeWrite)
	var attempts int
	if w.s != nil {
		row, err := gen.Generate()
		if err != nil {
			log.Printf("generate error: %v", err)

			return err
		}

		attempts, finalErr = w.s.Write(ctx, row)
	} else {
		rows := make([]generator.Row, 0, w.cfg.BatchSize)
		for range w.cfg.BatchSize {
			row, err := gen.Generate()
			if err != nil {
				log.Printf("generate error: %v", err)

				return err
			}
			rows = append(rows, row)
		}

		attempts, finalErr = w.sb.WriteBatch(ctx, rows)
	}
	m.Finish(finalErr, attempts)

	return finalErr
}
