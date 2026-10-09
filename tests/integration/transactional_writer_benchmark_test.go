//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// Transactional writer benchmark: one operation is one Query transaction.
// Each operation creates a Topic writer with LazyTx, executes an UPSERT,
// writes a 1024-byte message, and commits. The writer is created before
// UPSERT so Write exercises UnLazyTX. Any failed operation fails the benchmark.
//
// Each parent benchmark prepares its topics before measurement and drops them
// afterward. It also removes its rows from the shared state table.
// Each operation has a 10-second deadline, shared across DoTx retries.
// The benchmark has no separate run deadline.

// BenchmarkTransactionalWriterSingle measures a single-partition writer.
func BenchmarkTransactionalWriterSingle(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "single", Mode: txWriterWriterModeSingle, Routing: txWriterRoutingModeKey,
	})
}

// BenchmarkTransactionalWriterManyKey measures keyed multi-partition writing.
func BenchmarkTransactionalWriterManyKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeKey,
	})
}

// BenchmarkTransactionalWriterManyBoundedKey measures bounded-key multi-partition writing.
func BenchmarkTransactionalWriterManyBoundedKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-bounded-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeBoundedKey,
	})
}

// BenchmarkTransactionalWriterAutoSplit measures bounded-key writing after YDB splits a partition.
// The setup waits for two active partitions, and the partition cap prevents
// another topology change during measurement.
func BenchmarkTransactionalWriterAutoSplit(b *testing.B) {
	scope := newScope(b)
	ctx := scope.Ctx
	db := scope.Driver()

	benchmarkCase := txWriterStandardBenchmarkCase{
		Name:    "many-bounded-autosplit",
		Mode:    txWriterWriterModeMany,
		Routing: txWriterRoutingModeBoundedKey,
	}
	topicPath := txWriterStandardBenchmarkTopicPrefix + "-autosplit"
	cfg := txWriterNewStandardBenchmarkConfig(topicPath, benchmarkCase, 1)
	cfg.AutoSplit = true
	cfg.AutoSplitWriteSpeed = 1 << 20
	cfg.AutoSplitBurstBytes = 1 << 20
	cfg.AutoSplitUpUtilization = 2
	cfg.AutoSplitStabilization = 2 * time.Second

	txWriterPrepareBenchmark(b, ctx, db, cfg)
	txWriterWaitForAutoSplit(b, ctx, db, cfg)
	b.Run(fmt.Sprintf("active-partitions-%d", txWriterAutoSplitBenchmarkPartitions), func(b *testing.B) {
		txWriterRunBenchmark(b, ctx, db, cfg)
		activePartitions, err := txWriterActivePartitionCount(ctx, db, cfg.TopicPath)
		if err != nil {
			b.Fatal(err)
		}
		if activePartitions != txWriterAutoSplitBenchmarkPartitions {
			b.Fatalf("active partitions after measurement: got %d, want %d", activePartitions, txWriterAutoSplitBenchmarkPartitions)
		}
	})
}

const (
	txWriterStandardBenchmarkTopicPrefix = "tx-writer-benchmark"
	txWriterStandardBenchmarkTable       = "tx-writer-benchmark-state"
	txWriterAutoSplitBenchmarkPartitions = 2
)

const txWriterTransactionTimeout = 10 * time.Second

type txWriterStandardBenchmarkCase struct {
	Name    string
	Mode    txWriterWriterMode
	Routing txWriterRoutingMode
}

func txWriterNewStandardBenchmarkConfig(
	topicPath string,
	benchmarkCase txWriterStandardBenchmarkCase,
	partitionCount int64,
) txWriterConfig {
	runID := txWriterDefaultRunID()

	return txWriterConfig{
		TopicPath:         topicPath + "-" + runID,
		TablePath:         txWriterStandardBenchmarkTable,
		RunID:             runID,
		ProducerIDPrefix:  runID,
		Mode:              benchmarkCase.Mode,
		Routing:           benchmarkCase.Routing,
		Concurrency:       runtime.GOMAXPROCS(0),
		MessageSize:       1024,
		PreparePartitions: partitionCount,
	}
}

func txWriterPrepareBenchmark(b *testing.B, ctx context.Context, db *ydb.Driver, cfg txWriterConfig) {
	b.Helper()

	if err := txWriterPrepareSchema(ctx, db, cfg); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		deleteQuery := fmt.Sprintf(
			"DELETE FROM %s WHERE run_id = $run_id;",
			txWriterQuoteYQLPath(cfg.TablePath),
		)
		if err := db.Query().Exec(
			ctx,
			deleteQuery,
			query.WithParameters(ydb.ParamsBuilder().Param("$run_id").Text(cfg.RunID).Build()),
			query.WithIdempotent(),
		); err != nil {
			b.Errorf("delete benchmark state rows for run %q: %v", cfg.RunID, err)
		}
		if err := db.Topic().Drop(ctx, cfg.TopicPath); err != nil {
			b.Errorf("drop benchmark topic %q: %v", cfg.TopicPath, err)
		}
	})
}

func txWriterRunBenchmark(b *testing.B, ctx context.Context, db *ydb.Driver, cfg txWriterConfig) {
	b.Helper()

	payload := txWriterMakePayload(cfg.MessageSize)
	runners := txWriterNewTransactionRunners(db, cfg, payload)
	var warmup sync.WaitGroup
	warmupErrors := make(chan error, len(runners))
	warmup.Add(len(runners))
	for _, runner := range runners {
		go func() {
			defer warmup.Done()
			if err := runner.execute(ctx, 0); err != nil {
				warmupErrors <- err
			}
		}()
	}
	warmup.Wait()
	close(warmupErrors)
	for err := range warmupErrors {
		b.Fatalf("benchmark warmup failed: %v", err)
	}
	workerStats := make([]txWriterWorkerStats, cfg.Concurrency)
	b.Run("transactions", func(b *testing.B) {
		b.ReportAllocs()
		if err := txWriterRunParallelTransactions(ctx, b, runners, workerStats); err != nil {
			b.Fatalf("benchmark transaction failed: %v", err)
		}
	})
}

func txWriterWaitForAutoSplit(b *testing.B, ctx context.Context, db *ydb.Driver, cfg txWriterConfig) {
	b.Helper()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	prefillConfig := cfg
	prefillConfig.ProducerIDPrefix += "-prefill"
	runners := txWriterNewTransactionRunners(db, prefillConfig, txWriterMakePayload(cfg.MessageSize))
	for batch := uint64(0); ; batch++ {
		var workers sync.WaitGroup
		errors := make(chan error, len(runners))
		workers.Add(len(runners))
		for _, runner := range runners {
			go func() {
				defer workers.Done()
				for number := batch*8 + 1; number <= (batch+1)*8; number++ {
					if err := runner.execute(ctx, number); err != nil {
						errors <- err
						return
					}
				}
			}()
		}
		workers.Wait()
		close(errors)
		for err := range errors {
			b.Fatalf("write before auto split: %v", err)
		}
		activePartitions, err := txWriterActivePartitionCount(ctx, db, cfg.TopicPath)
		if err != nil {
			b.Fatal(err)
		}
		if activePartitions == txWriterAutoSplitBenchmarkPartitions {
			return
		}
	}
}

func txWriterActivePartitionCount(ctx context.Context, db *ydb.Driver, topicPath string) (int, error) {
	description, err := db.Topic().Describe(ctx, topicPath)
	if err != nil {
		return 0, fmt.Errorf("describe benchmark topic %q: %w", topicPath, err)
	}
	activePartitions := 0
	for _, partition := range description.Partitions {
		if partition.Active {
			activePartitions++
		}
	}
	return activePartitions, nil
}

func txWriterRunParallelTransactions(
	ctx context.Context,
	b *testing.B,
	runners []*txWriterTransactionRunner,
	allStats []txWriterWorkerStats,
) error {
	var workerCounter atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(workerCounter.Add(1) - 1)
		runner := runners[workerID]
		stats := &allStats[workerID]
		for pb.Next() {
			transactionNumber := stats.LogicalTransactions + 1
			stats.LogicalTransactions++
			err := runner.execute(ctx, transactionNumber)
			if err != nil {
				stats.FirstError = err
				// RunParallel requires each worker to exhaust pb.Next.
				for pb.Next() {
				}
				return
			}
		}
	})

	for i := range allStats {
		if allStats[i].FirstError != nil {
			return allStats[i].FirstError
		}
	}

	return nil
}

type txWriterWriterMode string

const (
	txWriterWriterModeSingle txWriterWriterMode = "single"
	txWriterWriterModeMany   txWriterWriterMode = "many"
)

type txWriterRoutingMode string

const (
	txWriterRoutingModeKey        txWriterRoutingMode = "key"
	txWriterRoutingModeBoundedKey txWriterRoutingMode = "bounded-key"
)

type txWriterConfig struct {
	TopicPath              string
	TablePath              string
	RunID                  string
	ProducerIDPrefix       string
	Mode                   txWriterWriterMode
	Routing                txWriterRoutingMode
	Concurrency            int
	MessageSize            int
	PreparePartitions      int64
	AutoSplit              bool
	AutoSplitWriteSpeed    int64
	AutoSplitBurstBytes    int64
	AutoSplitUpUtilization int
	AutoSplitStabilization time.Duration
}

func txWriterDefaultRunID() string {
	return fmt.Sprintf(
		"go-%s-%d",
		time.Now().UTC().Format("20060102T150405.000000000Z"),
		os.Getpid(),
	)
}

type txWriterWorkerStats struct {
	LogicalTransactions uint64
	FirstError          error
}

func txWriterRunFixedPartitionBenchmark(b *testing.B, benchmarkCase txWriterStandardBenchmarkCase) {
	b.Helper()
	scope := newScope(b)
	ctx := scope.Ctx
	db := scope.Driver()

	configs := make([]txWriterConfig, 0, 4)
	for _, partitionCount := range [...]int64{64, 128, 256, 512} {
		cfg := txWriterNewStandardBenchmarkConfig(
			fmt.Sprintf("%s-%s-p%d", txWriterStandardBenchmarkTopicPrefix, benchmarkCase.Name, partitionCount),
			benchmarkCase,
			partitionCount,
		)
		txWriterPrepareBenchmark(b, ctx, db, cfg)
		configs = append(configs, cfg)
	}

	for _, cfg := range configs {
		b.Run(fmt.Sprintf("p%d", cfg.PreparePartitions), func(b *testing.B) {
			txWriterRunBenchmark(b, ctx, db, cfg)
		})
	}
}

const txWriterBenchmarkTableQueryTemplate = `
UPSERT INTO %s (run_id, worker_id, updated_at)
VALUES ($run_id, $worker_id, CurrentUtcTimestamp());
`

func txWriterTopicAutoPartitioningSettings(cfg txWriterConfig) topictypes.AutoPartitioningSettings {
	settings := topictypes.AutoPartitioningSettings{
		AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyDisabled,
	}
	if cfg.Routing == txWriterRoutingModeBoundedKey {
		settings.AutoPartitioningStrategy = topictypes.AutoPartitioningStrategyPaused
	}
	if cfg.AutoSplit {
		settings.AutoPartitioningStrategy = topictypes.AutoPartitioningStrategyScaleUp
		settings.AutoPartitioningWriteSpeedStrategy = topictypes.AutoPartitioningWriteSpeedStrategy{
			StabilizationWindow:  cfg.AutoSplitStabilization,
			UpUtilizationPercent: int32(cfg.AutoSplitUpUtilization),
		}
	}

	return settings
}

func txWriterPrepareSchema(ctx context.Context, db *ydb.Driver, cfg txWriterConfig) error {
	statement := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    run_id Utf8 NOT NULL,
    worker_id Uint64 NOT NULL,
    updated_at Timestamp,
    PRIMARY KEY (run_id, worker_id)
);
`, txWriterQuoteYQLPath(cfg.TablePath))
	if err := db.Query().Exec(ctx, statement, query.WithIdempotent()); err != nil {
		return fmt.Errorf("prepare table %q: %w", cfg.TablePath, err)
	}

	_, err := db.Topic().Describe(ctx, cfg.TopicPath)
	if err == nil {
		return nil
	}
	if !ydb.IsOperationErrorNotFoundError(err) && !ydb.IsOperationErrorSchemeError(err) {
		return fmt.Errorf("describe topic %q before prepare: %w", cfg.TopicPath, err)
	}

	createOptions := []topicoptions.CreateOption{
		topicoptions.CreateWithMinActivePartitions(cfg.PreparePartitions),
		topicoptions.CreateWithAutoPartitioningSettings(txWriterTopicAutoPartitioningSettings(cfg)),
	}
	if cfg.AutoSplit {
		createOptions = append(
			createOptions,
			// Keep the measured topology fixed after the initial split.
			topicoptions.CreateWithMaxActivePartitions(txWriterAutoSplitBenchmarkPartitions),
			topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(cfg.AutoSplitWriteSpeed),
			topicoptions.CreateWithPartitionWriteBurstBytes(cfg.AutoSplitBurstBytes),
		)
	} else {
		createOptions = append(createOptions, topicoptions.CreateWithMaxActivePartitions(cfg.PreparePartitions))
	}

	err = db.Topic().Create(ctx, cfg.TopicPath, createOptions...)
	if err != nil && !ydb.IsOperationErrorAlreadyExistsError(err) {
		return fmt.Errorf("prepare topic %q: %w", cfg.TopicPath, err)
	}

	return nil
}

func txWriterMakePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	return payload
}

type txWriterTransactionRunner struct {
	db               *ydb.Driver
	topicPath        string
	tableQuery       string
	runID            string
	workerID         int
	payload          []byte
	writerConfig     txWriterConfig
	doTxOptions      []query.DoTxOption
	messageKeyPrefix string
	execute          func(context.Context, uint64) error
}

func txWriterNewTransactionRunner(
	db *ydb.Driver,
	cfg txWriterConfig,
	workerID int,
	payload []byte,
) *txWriterTransactionRunner {
	runner := &txWriterTransactionRunner{
		db:           db,
		topicPath:    cfg.TopicPath,
		tableQuery:   fmt.Sprintf(txWriterBenchmarkTableQueryTemplate, txWriterQuoteYQLPath(cfg.TablePath)),
		runID:        cfg.RunID,
		workerID:     workerID,
		payload:      payload,
		writerConfig: cfg,
		doTxOptions: []query.DoTxOption{
			query.WithIdempotent(),
			query.WithLazyTx(true),
		},
	}
	if cfg.Mode == txWriterWriterModeMany {
		runner.messageKeyPrefix = fmt.Sprintf("worker-%d-message-", workerID)
		runner.execute = runner.executeManyWriterTransaction
	} else {
		runner.execute = runner.executeSingleWriterTransaction
	}

	return runner
}

func txWriterNewTransactionRunners(db *ydb.Driver, cfg txWriterConfig, payload []byte) []*txWriterTransactionRunner {
	runners := make([]*txWriterTransactionRunner, cfg.Concurrency)
	for workerID := range runners {
		runners[workerID] = txWriterNewTransactionRunner(db, cfg, workerID, payload)
	}

	return runners
}

func (r *txWriterTransactionRunner) executeSingleWriterTransaction(
	ctx context.Context,
	_ uint64,
) error {
	return r.executeTransaction(ctx, "")
}

func (r *txWriterTransactionRunner) executeManyWriterTransaction(
	ctx context.Context,
	transactionNumber uint64,
) error {
	return r.executeTransaction(ctx, r.messageKey(transactionNumber))
}

func (r *txWriterTransactionRunner) messageKey(transactionNumber uint64) string {
	return r.messageKeyPrefix + strconv.FormatUint(transactionNumber, 10)
}

func (r *txWriterTransactionRunner) executeTransaction(
	ctx context.Context,
	messageKey string,
) error {
	txCtx, cancel := context.WithTimeout(ctx, txWriterTransactionTimeout)
	defer cancel()

	return r.db.Query().DoTx(
		txCtx,
		func(ctx context.Context, tx query.TxActor) error {
			// Partition choosers keep mutable partition state. DoTx may call this
			// callback more than once, so each writer needs a fresh chooser.
			writerOptions := txWriterWriterOptions(r.writerConfig, r.workerID)
			writer, err := r.db.Topic().StartTransactionalWriter(
				tx,
				r.topicPath,
				writerOptions...,
			)
			if err != nil {
				return fmt.Errorf("start transactional writer: %w", err)
			}

			err = tx.Exec(
				ctx,
				r.tableQuery,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$run_id").Text(r.runID).
						Param("$worker_id").Uint64(uint64(r.workerID)).
						Build(),
				),
			)
			if err != nil {
				return fmt.Errorf("execute benchmark table upsert: %w", err)
			}

			err = writer.Write(ctx, topicwriter.Message{
				Key:  messageKey,
				Data: bytes.NewReader(r.payload),
			})
			if err != nil {
				return fmt.Errorf("write transactional topic messages: %w", err)
			}

			return nil
		},
		r.doTxOptions...,
	)
}

func txWriterWriterOptions(cfg txWriterConfig, workerID int) []topicoptions.WriterOption {
	options := []topicoptions.WriterOption{
		topicoptions.WithWriterDirectWrite(false),
	}
	slotProducerID := ""
	if cfg.ProducerIDPrefix != "" {
		slotProducerID = fmt.Sprintf("%s-slot-%d", cfg.ProducerIDPrefix, workerID)
	}

	if cfg.Mode == txWriterWriterModeSingle {
		if slotProducerID != "" {
			options = append(options, topicoptions.WithWriterProducerID(slotProducerID))
		}

		return options
	}

	multiWriterOptions := []topicoptions.MultiWriterOption{
		topicoptions.WithMultiWriterDirectWrite(false),
	}
	switch cfg.Routing {
	case txWriterRoutingModeKey:
		multiWriterOptions = append(
			multiWriterOptions,
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
		)
	case txWriterRoutingModeBoundedKey:
		multiWriterOptions = append(
			multiWriterOptions,
			topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser()),
		)
	}
	if slotProducerID != "" {
		multiWriterOptions = append(multiWriterOptions, topicoptions.WithProducerIDPrefix(slotProducerID))
	}

	return append(options, topicoptions.WithWriteToManyPartitions(multiWriterOptions...))
}

func txWriterQuoteYQLPath(path string) string {
	return "`" + path + "`"
}
