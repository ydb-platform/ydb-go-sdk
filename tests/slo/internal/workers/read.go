package workers

import (
	"context"
	"math/rand"
	"sync"

	"golang.org/x/time/rate"

	"slo/internal/log"
	"slo/internal/metrics"
)

func (w *Workers) Read(ctx context.Context, wg *sync.WaitGroup, rl *rate.Limiter) {
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

			err = w.read(ctx)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr == nil {
					log.Printf("read failed: %v", err)
				}
			}
		}
	}
}

func (w *Workers) ReadID() uint64 {
	if w.Gen == nil {
		return uint64(rand.Intn(int(w.cfg.InitialDataCount))) //nolint: gosec
	}
	row, err := w.Gen.Generate()
	if err != nil {
		log.Panicf("generate error: %v", err)
	}
	return row.ID
}

func (w *Workers) ReadIDs() []uint64 {
	ids := make([]uint64, 0, w.cfg.BatchSize)
	for range w.cfg.BatchSize {
		if w.Gen == nil {
			ids = append(ids, uint64(rand.Intn(int(w.cfg.InitialDataCount)))) //nolint: gosec
		} else {
			row, err := w.Gen.Generate()
			if err != nil {
				log.Panicf("generate error: %v", err)
			}
			ids = append(ids, row.ID)
		}
	}
	return ids
}

func (w *Workers) read(ctx context.Context) error {
	var m metrics.Span
	var attempts int
	var err error
	if w.s != nil {
		id := w.ReadID()
		m = w.m.Start(metrics.OperationTypeRead)
		_, attempts, err = w.s.Read(ctx, id)
	} else {
		ids := w.ReadIDs()
		m = w.m.Start(metrics.OperationTypeRead)
		_, attempts, err = w.sb.ReadBatch(ctx, ids)
	}

	m.Finish(err, attempts)

	return err
}
