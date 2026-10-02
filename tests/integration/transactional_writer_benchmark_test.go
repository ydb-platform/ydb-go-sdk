//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
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
// Configure the integration scope connection and credentials for the target YDB.
// Fixed-partition benchmark:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriter(Single|ManyKey|ManyBoundedKey)$' \
//	  -count=3 -cpu=4
//
// Auto-split benchmark:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterAutoSplit$' \
//	  -benchtime=300x -count=3 -cpu=4
//
// Each invocation creates a unique topic and drops it on normal completion.
// Each operation has a 10-second deadline, shared across DoTx retries.
// The benchmark has no separate run deadline.
//
// Baseline: 2026-10-02, Go 1.26.4, Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Each transaction had a 10-second deadline; no operation failed.

/*
BenchmarkTransactionalWriterSingle/p64-4       	      43	  23399800 ns/op
BenchmarkTransactionalWriterSingle/p64-4       	      52	  23673329 ns/op
BenchmarkTransactionalWriterSingle/p64-4       	      45	  22694831 ns/op
BenchmarkTransactionalWriterSingle/p128-4      	      45	  24999329 ns/op
BenchmarkTransactionalWriterSingle/p128-4      	      45	  22980434 ns/op
BenchmarkTransactionalWriterSingle/p128-4      	      44	  23262187 ns/op
BenchmarkTransactionalWriterSingle/p256-4      	      43	  24384788 ns/op
BenchmarkTransactionalWriterSingle/p256-4      	      43	  24057471 ns/op
BenchmarkTransactionalWriterSingle/p256-4      	      50	  24257687 ns/op
BenchmarkTransactionalWriterSingle/p512-4      	      50	  23691647 ns/op
BenchmarkTransactionalWriterSingle/p512-4      	      46	  24156962 ns/op
BenchmarkTransactionalWriterSingle/p512-4      	      45	  24110108 ns/op
*/
// BenchmarkTransactionalWriterSingle measures a single-partition writer.
func BenchmarkTransactionalWriterSingle(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "single", Mode: txWriterWriterModeSingle, Routing: txWriterRoutingModeKey,
	})
}

/*
BenchmarkTransactionalWriterManyKey/p64-4      	       9	 127784208 ns/op
BenchmarkTransactionalWriterManyKey/p64-4      	       9	 142928583 ns/op
BenchmarkTransactionalWriterManyKey/p64-4      	      10	 125922075 ns/op
BenchmarkTransactionalWriterManyKey/p128-4     	       1	1477367042 ns/op
BenchmarkTransactionalWriterManyKey/p128-4     	       4	 355355823 ns/op
BenchmarkTransactionalWriterManyKey/p128-4     	       1	1027614042 ns/op
BenchmarkTransactionalWriterManyKey/p256-4     	       1	1868199709 ns/op
BenchmarkTransactionalWriterManyKey/p256-4     	       2	 521324125 ns/op
BenchmarkTransactionalWriterManyKey/p256-4     	       2	 556673917 ns/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	1606657375 ns/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	1549169875 ns/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	1544174791 ns/op
*/
// BenchmarkTransactionalWriterManyKey measures keyed multi-partition writing.
func BenchmarkTransactionalWriterManyKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeKey,
	})
}

/*
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       9	 127129444 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       9	 127169787 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       9	 125453593 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       5	 257912083 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       6	 175197021 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       7	 154930804 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       2	 629736146 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       2	 651915458 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       1	1031985709 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1777262167 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	2003394458 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1807805083 ns/op
*/
// BenchmarkTransactionalWriterManyBoundedKey measures bounded-key multi-partition writing.
func BenchmarkTransactionalWriterManyBoundedKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-bounded-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeBoundedKey,
	})
}

/*
BenchmarkTransactionalWriterAutoSplit-4   	     300	  34114646 ns/op
BenchmarkTransactionalWriterAutoSplit-4   	     300	  30775598 ns/op
BenchmarkTransactionalWriterAutoSplit-4   	     300	  31640869 ns/op
*/
// BenchmarkTransactionalWriterAutoSplit measures bounded-key writing while YDB may split partitions.
func BenchmarkTransactionalWriterAutoSplit(b *testing.B) {
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

	txWriterRunBenchmark(b, cfg)
}

const (
	txWriterStandardBenchmarkTopicPrefix = "tx-writer-benchmark"
	txWriterStandardBenchmarkTable       = "tx-writer-benchmark-state"
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

func txWriterRunBenchmark(b *testing.B, cfg txWriterConfig) {
	b.Helper()
	b.StopTimer()

	scope := newScope(b)
	ctx := scope.Ctx
	db := scope.Driver()

	if err := txWriterPrepareSchema(ctx, db, cfg); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Topic().Drop(ctx, cfg.TopicPath); err != nil {
			b.Errorf("drop benchmark topic %q: %v", cfg.TopicPath, err)
		}
	})
	payload := txWriterMakePayload(cfg.MessageSize)
	runners := txWriterNewTransactionRunners(db, cfg, payload)
	workerStats := make([]txWriterWorkerStats, cfg.Concurrency)
	b.ResetTimer()
	b.StartTimer()
	err := txWriterRunParallelTransactions(ctx, b, runners, workerStats)
	b.StopTimer()
	if err != nil {
		b.Fatalf("benchmark transaction failed: %v", err)
	}
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
	for _, partitionCount := range [...]int64{64, 128, 256, 512} {
		b.Run(fmt.Sprintf("p%d", partitionCount), func(b *testing.B) {
			cfg := txWriterNewStandardBenchmarkConfig(
				fmt.Sprintf("%s-%s-p%d", txWriterStandardBenchmarkTopicPrefix, benchmarkCase.Name, partitionCount),
				benchmarkCase,
				partitionCount,
			)
			txWriterRunBenchmark(b, cfg)
		})
	}
}

const txWriterBenchmarkTableQueryTemplate = `
UPSERT INTO %s (run_id, worker_id, updated_at)
VALUES ($run_id, $worker_id, CurrentUtcTimestamp());
`

const ydbMaxTopicPartitions int64 = 35_000

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
			// YDB treats an omitted maximum as equal to the minimum, which would
			// disable splitting. Use the server-wide ceiling so the benchmark does
			// not introduce a lower partition limit of its own.
			topicoptions.CreateWithMaxActivePartitions(ydbMaxTopicPartitions),
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
