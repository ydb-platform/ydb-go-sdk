package kv

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"

	"slo/internal/framework"
	"slo/internal/generator"
)

// Database is the interface each workload must implement.
// It provides only the database-specific operations; everything else
// (flag parsing, table path, prefill loop, run loop) is handled by Workload.
type Database interface {
	CreateTable(ctx context.Context) error
	DropTable(ctx context.Context) error
	Read(ctx context.Context, id generator.RowID) (generator.Row, int, error)
	Write(ctx context.Context, row generator.Row) (int, error)
	Close(ctx context.Context) error
}

// RetryBudget is the interface returned by budget.Limited.
type RetryBudget interface {
	budget.Budget
	Stop()
}

// Params holds everything parsed and computed from CLI flags + framework config.
// A single call to ParseParams is the only thing each workload needs before
// opening its DB connection.
type Params struct {
	ReadRPS           uint
	WriteRPS          uint
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	PrefillCount      uint64
	TablePath         string // path.Join(fw.Config.Database, fw.Config.Label, fw.Config.Ref)
	PartitionSize     uint   // MB
	MinPartitionCount uint
	MaxPartitionCount uint
	RetryBudget       RetryBudget // budget.Limited(PoolSize * 0.1)
}

// PoolSize returns ReadRPS + WriteRPS, the recommended connection pool size.
func (p Params) PoolSize() int { return int(p.ReadRPS + p.WriteRPS) }

// ParseParams parses the standard KV workload flags and computes derived fields
// (TablePath, RetryBudget). The extraFlags callback, if non-nil, is called before
// fs.Parse so workload-specific flags can be registered on the same FlagSet.
func ParseParams(fw *framework.Framework, name string, extraFlags func(*flag.FlagSet)) Params {
	var (
		readRPS           uint
		writeRPS          uint
		readTimeout       uint
		writeTimeout      uint
		prefillCount      uint64
		partitionSize     uint
		minPartitionCount uint
		maxPartitionCount uint
	)

	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.UintVar(&readRPS, "read-rps", 1000, "read RPS")
	fs.UintVar(&writeRPS, "write-rps", 100, "write RPS")
	fs.UintVar(&readTimeout, "read-timeout", 10000, "read timeout in milliseconds")
	fs.UintVar(&writeTimeout, "write-timeout", 10000, "write timeout in milliseconds")
	fs.Uint64Var(&prefillCount, "prefill-count", 1000, "prefill rows count")
	fs.UintVar(&partitionSize, "partition-size", 1, "partition size in MB")
	fs.UintVar(&minPartitionCount, "min-partition-count", 6, "min partition count")
	fs.UintVar(&maxPartitionCount, "max-partition-count", 1000, "max partition count")

	if extraFlags != nil {
		extraFlags(fs)
	}

	_ = fs.Parse(os.Args[1:])

	poolSize := readRPS + writeRPS

	return Params{
		ReadRPS:           readRPS,
		WriteRPS:          writeRPS,
		ReadTimeout:       time.Duration(readTimeout) * time.Millisecond,
		WriteTimeout:      time.Duration(writeTimeout) * time.Millisecond,
		PrefillCount:      prefillCount,
		TablePath:         path.Join(fw.Config.Database, fw.Config.Label, fw.Config.Ref),
		PartitionSize:     partitionSize,
		MinPartitionCount: minPartitionCount,
		MaxPartitionCount: maxPartitionCount,
		RetryBudget:       budget.Limited(int(float64(poolSize) * 0.1)), //nolint:mnd
	}
}

// Workload implements framework.Workload for the KV workload pattern.
type Workload struct {
	fw     *framework.Framework
	db     Database
	params Params
}

var _ framework.Workload = (*Workload)(nil)

// New creates a KV workload. The caller is responsible for opening the DB
// connection (using params.RetryBudget, params.PoolSize(), etc.) and
// implementing the Database interface.
func New(fw *framework.Framework, params Params, db Database) *Workload {
	return &Workload{fw: fw, db: db, params: params}
}

func (w *Workload) Setup(ctx context.Context) error {
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
			_, err = w.db.Write(ctx, row)

			return err
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("fill initial data failed: %w", err)
	}
	w.fw.Logger.Printf("entries write ok")

	return nil
}

func (w *Workload) Run(ctx context.Context) error {
	runGen := generator.New(w.params.PrefillCount)

	readRL := framework.NewRateLimiter(int(w.params.ReadRPS))
	writeRL := framework.NewRateLimiter(int(w.params.WriteRPS))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		framework.RunWorkers(ctx, int(w.params.ReadRPS), readRL, func(ctx context.Context) {
			id := uint64(rand.Intn(int(w.params.PrefillCount))) //nolint:gosec
			span := w.fw.Metrics.Start(framework.OperationTypeRead)
			_, attempts, err := w.db.Read(ctx, id)
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				w.fw.Logger.Errorf("read failed: %v", err)
			}
		})
	}()

	go func() {
		defer wg.Done()
		framework.RunWorkers(ctx, int(w.params.WriteRPS), writeRL, func(ctx context.Context) {
			row, err := runGen.Generate()
			if err != nil {
				w.fw.Logger.Errorf("generate error: %v", err)

				return
			}
			span := w.fw.Metrics.Start(framework.OperationTypeWrite)
			attempts, err := w.db.Write(ctx, row)
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				w.fw.Logger.Errorf("write failed: %v", err)
			}
		})
	}()

	wg.Wait()

	return nil
}

func (w *Workload) Teardown(ctx context.Context) error {
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
