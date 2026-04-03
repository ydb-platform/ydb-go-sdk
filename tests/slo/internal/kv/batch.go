package kv

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"golang.org/x/sync/errgroup"

	"slo/internal/framework"
	"slo/internal/generator"
)

// BatchDatabase is the interface for workloads that operate on batches of rows.
// It provides only the database-specific operations; everything else
// (flag parsing, prefill loop, run loop) is handled by BatchWorkload.
type BatchDatabase interface {
	CreateTable(ctx context.Context) error
	DropTable(ctx context.Context) error
	ReadBatch(ctx context.Context, ids []generator.RowID) ([]generator.Row, int, error)
	WriteBatch(ctx context.Context, rows []generator.Row) (int, error)
	Close(ctx context.Context) error
}

// BatchWorkload implements framework.Workload for batch-oriented workloads
// such as bulk upsert. Each read/write operation processes batchSize rows at once.
type BatchWorkload struct {
	fw        *framework.Framework
	db        BatchDatabase
	params    Params
	batchSize uint
}

var _ framework.Workload = (*BatchWorkload)(nil)

// NewBatch creates a batch KV workload. The caller is responsible for opening
// the DB connection and implementing the BatchDatabase interface.
func NewBatch(fw *framework.Framework, params Params, batchSize uint, db BatchDatabase) *BatchWorkload {
	return &BatchWorkload{fw: fw, db: db, params: params, batchSize: batchSize}
}

func (w *BatchWorkload) Setup(ctx context.Context) error {
	if err := w.db.CreateTable(ctx); err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	w.fw.Logger.Printf("create table ok")

	gen := generator.New(0)
	g := errgroup.Group{}
	for i := uint64(0); i < w.params.PrefillCount; i++ {
		g.Go(func() error {
			row, err := gen.Generate()
			if err != nil {
				return err
			}
			_, err = w.db.WriteBatch(ctx, []generator.Row{row})

			return err
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("fill initial data failed: %w", err)
	}
	w.fw.Logger.Printf("entries write ok")

	return nil
}

func (w *BatchWorkload) Run(ctx context.Context) error {
	runGen := generator.New(w.params.PrefillCount)

	readRL := framework.NewRateLimiter(int(w.params.ReadRPS))
	writeRL := framework.NewRateLimiter(int(w.params.WriteRPS))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		framework.RunWorkers(ctx, int(w.params.ReadRPS), readRL, func(ctx context.Context) {
			ids := make([]generator.RowID, 0, w.batchSize)
			for range w.batchSize {
				ids = append(ids, uint64(rand.Intn(int(w.params.PrefillCount)))) //nolint:gosec
			}
			span := w.fw.Metrics.Start(framework.OperationTypeRead)
			_, attempts, err := w.db.ReadBatch(ctx, ids)
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				w.fw.Logger.Errorf("read failed: %v", err)
			}
		})
	}()

	go func() {
		defer wg.Done()
		framework.RunWorkers(ctx, int(w.params.WriteRPS), writeRL, func(ctx context.Context) {
			rows := make([]generator.Row, 0, w.batchSize)
			for range w.batchSize {
				row, err := runGen.Generate()
				if err != nil {
					w.fw.Logger.Errorf("generate error: %v", err)

					return
				}
				rows = append(rows, row)
			}
			span := w.fw.Metrics.Start(framework.OperationTypeWrite)
			attempts, err := w.db.WriteBatch(ctx, rows)
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				w.fw.Logger.Errorf("write failed: %v", err)
			}
		})
	}()

	wg.Wait()

	return nil
}

func (w *BatchWorkload) Teardown(ctx context.Context) error {
	defer func() {
		w.params.RetryBudget.Stop()
		_ = w.db.Close(ctx)
	}()

	if err := w.db.DropTable(ctx); err != nil {
		return fmt.Errorf("drop table failed: %w", err)
	}
	w.fw.Logger.Printf("cleanup table ok")

	return nil
}
