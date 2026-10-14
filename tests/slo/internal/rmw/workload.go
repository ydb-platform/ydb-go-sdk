package rmw

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"slo/internal/framework"
	"slo/internal/kv"
)

// Database is implemented only by the native Query-service RMW adapter.
type Database interface {
	Setup(ctx context.Context, counters uint64) error
	Apply(ctx context.Context, op Operation) (int, error)
	Snapshot(ctx context.Context) (State, int, error)
	Drop(ctx context.Context) error
	Close(ctx context.Context) error
}

// Workload shares the ordinary SLO lifecycle and metrics, not the KV operations.
type Workload struct {
	fw       *framework.Framework
	params   kv.Params
	db       Database
	workers  int
	counters uint64
}

func New(fw *framework.Framework, params kv.Params, db Database, workers int, counters uint64) *Workload {
	return &Workload{fw: fw, params: params, db: db, workers: workers, counters: counters}
}

func (w *Workload) Setup(ctx context.Context) error {
	if w.workers < 2 || w.counters == 0 || w.params.WriteRPS == 0 {
		return fmt.Errorf("RMW requires at least two workers, positive counters and positive write-rps")
	}

	return w.db.Setup(ctx, w.counters)
}

func (w *Workload) Run(ctx context.Context) error {
	expected, finalErr := schedule(ctx, w.workers, w.counters,
		framework.NewRateLimiter(int(w.params.WriteRPS)),
		func(ctx context.Context, op Operation) error {
			span := w.fw.Metrics.Start(framework.OperationTypeWrite)
			attempts, err := w.db.Apply(ctx, op)
			span.Finish(err, attempts)

			return err
		},
	)

	// Scheduling cancellation must not cancel transactions already admitted. All
	// calls have settled here, each under the adapter's ordinary write timeout.
	// Read fresh database state independently even after a final transaction error.
	span := w.fw.Metrics.Start(framework.OperationTypeRead)
	state, attempts, checkErr := w.db.Snapshot(context.WithoutCancel(ctx))
	if checkErr == nil {
		checkErr = Check(expected, w.counters, state)
	}
	span.Finish(checkErr, attempts)
	if finalErr != nil {
		if checkErr != nil {
			w.fw.Logger.Errorf("RMW diagnostic check failed: %v", checkErr)
		}

		return finalErr
	}

	return checkErr
}

func (w *Workload) Teardown(ctx context.Context) error {
	defer func() {
		w.params.RetryBudget.Stop()
		_ = w.db.Close(ctx)
	}()

	return w.db.Drop(ctx)
}

// schedule stops admitting work on cancellation, but deliberately does not pass
// that cancellation into admitted operations. No final operation error is masked
// as planned shutdown, including an error returned after the scheduling stop.
func schedule(ctx context.Context, workers int, counters uint64, limiter *rate.Limiter,
	apply func(context.Context, Operation) error,
) ([]Operation, error) {
	group, schedulingCtx := errgroup.WithContext(ctx)
	var next atomic.Uint64
	operations := make([][]Operation, workers)
	operationCtx := context.WithoutCancel(ctx)
	for worker := range workers {
		group.Go(func() error {
			for {
				select {
				case <-schedulingCtx.Done():
					return nil
				default:
				}
				err := limiter.Wait(schedulingCtx)
				if err != nil && errors.Is(err, schedulingCtx.Err()) {
					return nil
				}
				if err != nil {
					return err
				}
				select {
				case <-schedulingCtx.Done():
					return nil
				default:
				}
				id := next.Add(1)
				op := Operation{ID: id, Counter: (id - 1) % counters, Delta: 1}
				operations[worker] = append(operations[worker], op)
				if err := apply(operationCtx, op); err != nil {
					return err
				}
			}
		})
	}
	err := group.Wait()
	var expected []Operation
	for _, ops := range operations {
		expected = append(expected, ops...)
	}

	return expected, err
}
