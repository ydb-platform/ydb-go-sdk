package kv

import (
	"context"
	"fmt"
	"math/rand"

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
	expected  []generator.Row
}

var _ framework.Workload = (*BatchWorkload)(nil)

// NewBatch creates a batch KV workload. The caller is responsible for opening
// the DB connection and implementing the BatchDatabase interface.
func NewBatch(fw *framework.Framework, params Params, batchSize uint, db BatchDatabase) *BatchWorkload {
	return &BatchWorkload{fw: fw, db: db, params: params, batchSize: batchSize}
}

func (w *BatchWorkload) Setup(ctx context.Context) error {
	if w.params.PrefillCount == 0 || w.batchSize == 0 {
		return fmt.Errorf("prefill-count and batch-size must be positive for controlled reads")
	}
	w.expected = make([]generator.Row, w.params.PrefillCount)

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
			row = ControlRow(row)
			expected := CloneRow(row)
			_, err = w.db.WriteBatch(ctx, []generator.Row{row})
			if err == nil {
				w.expected[row.ID] = expected
			}

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

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return framework.RunWorkers(ctx, int(w.params.ReadRPS), readRL, func(ctx context.Context) error {
			ids := make([]generator.RowID, 0, w.batchSize)
			expected := make([]generator.Row, 0, w.batchSize)
			for range w.batchSize {
				id := uint64(rand.Intn(int(w.params.PrefillCount))) //nolint:gosec
				ids = append(ids, id)
				expected = append(expected, w.expected[id])
			}
			span := w.fw.Metrics.Start(framework.OperationTypeRead)
			rows, attempts, err := w.db.ReadBatch(ctx, ids)
			if err == nil {
				err = CheckRows(expected, rows)
			}
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				w.fw.Logger.Errorf("read failed: %v", err)
			}

			return err
		})
	})

	group.Go(func() error {
		return framework.RunWorkers(ctx, int(w.params.WriteRPS), writeRL, func(ctx context.Context) error {
			rows := make([]generator.Row, 0, w.batchSize)
			for range w.batchSize {
				row, err := runGen.Generate()
				if err != nil {
					w.fw.Logger.Errorf("generate error: %v", err)

					return err
				}
				rows = append(rows, row)
			}
			span := w.fw.Metrics.Start(framework.OperationTypeWrite)
			attempts, err := w.db.WriteBatch(ctx, rows)
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				w.fw.Logger.Errorf("write failed: %v", err)
			}

			return err
		})
	})

	return group.Wait()
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
