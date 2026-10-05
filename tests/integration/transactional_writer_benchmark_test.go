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
// Each parent benchmark prepares its topics before measurement and drops them
// afterward. It also removes its rows from the shared state table.
// Each operation has a 10-second deadline, shared across DoTx retries.
// The benchmark has no separate run deadline.

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-05, Go 1.27.1 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterSingle$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterSingle/p64-4       	      48	  23889453 ns/op	   78291 B/op	    1242 allocs/op
BenchmarkTransactionalWriterSingle/p64-4       	      50	  23411710 ns/op	   77808 B/op	    1241 allocs/op
BenchmarkTransactionalWriterSingle/p64-4       	      49	  23004689 ns/op	   77906 B/op	    1241 allocs/op
BenchmarkTransactionalWriterSingle/p128-4      	      46	  23238680 ns/op	   78458 B/op	    1242 allocs/op
BenchmarkTransactionalWriterSingle/p128-4      	      43	  23386471 ns/op	   77757 B/op	    1242 allocs/op
BenchmarkTransactionalWriterSingle/p128-4      	      52	  22377686 ns/op	   77755 B/op	    1241 allocs/op
BenchmarkTransactionalWriterSingle/p256-4      	      46	  23632148 ns/op	   77797 B/op	    1241 allocs/op
BenchmarkTransactionalWriterSingle/p256-4      	      46	  23219518 ns/op	   77593 B/op	    1241 allocs/op
BenchmarkTransactionalWriterSingle/p256-4      	      46	  23649817 ns/op	   77598 B/op	    1241 allocs/op
BenchmarkTransactionalWriterSingle/p512-4      	      45	  23628352 ns/op	   77491 B/op	    1240 allocs/op
BenchmarkTransactionalWriterSingle/p512-4      	      46	  24408697 ns/op	   78076 B/op	    1242 allocs/op
BenchmarkTransactionalWriterSingle/p512-4      	      48	  22490911 ns/op	   77705 B/op	    1242 allocs/op
*/
// BenchmarkTransactionalWriterSingle measures a single-partition writer.
func BenchmarkTransactionalWriterSingle(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "single", Mode: txWriterWriterModeSingle, Routing: txWriterRoutingModeKey,
	})
}

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-05, Go 1.27.1 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterManyKey$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterManyKey/p64-4      	       8	 130323490 ns/op	 2233584 B/op	   33196 allocs/op
BenchmarkTransactionalWriterManyKey/p64-4      	       9	 132717532 ns/op	 2236381 B/op	   33224 allocs/op
BenchmarkTransactionalWriterManyKey/p64-4      	       9	 126536292 ns/op	 2244664 B/op	   33292 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4     	       6	 245919139 ns/op	 4463174 B/op	   65800 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4     	       6	 409303215 ns/op	 4421866 B/op	   65428 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4     	       3	 378757222 ns/op	 4465864 B/op	   65794 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4     	       2	 535159021 ns/op	 8843192 B/op	  130468 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4     	       2	 610078833 ns/op	 8844652 B/op	  130510 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4     	       2	 619463146 ns/op	 8846828 B/op	  130533 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	1727684750 ns/op	17649552 B/op	  260649 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	1897948625 ns/op	17612872 B/op	  260454 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	2380674708 ns/op	17639000 B/op	  260815 allocs/op
*/
// BenchmarkTransactionalWriterManyKey measures keyed multi-partition writing.
func BenchmarkTransactionalWriterManyKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeKey,
	})
}

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-05, Go 1.27.1 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterManyBoundedKey$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       7	 163805839 ns/op	 2298929 B/op	   33897 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       7	 161294512 ns/op	 2272589 B/op	   33612 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       8	 132987354 ns/op	 2282266 B/op	   33663 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       5	 310225292 ns/op	 4487283 B/op	   66202 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       4	 430267188 ns/op	 4518120 B/op	   66512 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       4	 293932323 ns/op	 4520344 B/op	   66440 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       1	1236404000 ns/op	 8909776 B/op	  131741 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       1	1160582833 ns/op	 8926152 B/op	  131869 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       1	1185910250 ns/op	 8939096 B/op	  131973 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1990103750 ns/op	17839800 B/op	  262958 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1979818792 ns/op	17823408 B/op	  263171 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1944340333 ns/op	17819568 B/op	  263172 allocs/op
*/
// BenchmarkTransactionalWriterManyBoundedKey measures bounded-key multi-partition writing.
func BenchmarkTransactionalWriterManyBoundedKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-bounded-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeBoundedKey,
	})
}

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-05, Go 1.27.1 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterAutoSplit$' \
//	  -benchtime=300x -count=3 -cpu=4

/*
BenchmarkTransactionalWriterAutoSplit/tx-4         	     300	  30704545 ns/op	  160715 B/op	    2501 allocs/op
BenchmarkTransactionalWriterAutoSplit/tx-4         	     300	  28699257 ns/op	  173742 B/op	    2709 allocs/op
BenchmarkTransactionalWriterAutoSplit/tx-4         	     300	  28877437 ns/op	  173551 B/op	    2708 allocs/op
*/
// BenchmarkTransactionalWriterAutoSplit measures bounded-key writing while YDB may split partitions.
func BenchmarkTransactionalWriterAutoSplit(b *testing.B) {
	b.StopTimer()
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
	b.Run("tx", func(b *testing.B) {
		txWriterRunBenchmark(b, ctx, db, cfg)
	})
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
	b.StopTimer()

	payload := txWriterMakePayload(cfg.MessageSize)
	runners := txWriterNewTransactionRunners(db, cfg, payload)
	workerStats := make([]txWriterWorkerStats, cfg.Concurrency)
	b.ReportAllocs()
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
	b.StopTimer()
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
	transactionNumber uint64,
) error {
	return r.executeTransaction(ctx, transactionNumber, "")
}

func (r *txWriterTransactionRunner) executeManyWriterTransaction(
	ctx context.Context,
	transactionNumber uint64,
) error {
	return r.executeTransaction(ctx, transactionNumber, r.messageKey(transactionNumber))
}

func (r *txWriterTransactionRunner) messageKey(transactionNumber uint64) string {
	return r.messageKeyPrefix + strconv.FormatUint(transactionNumber, 10)
}

func (r *txWriterTransactionRunner) executeTransaction(
	ctx context.Context,
	transactionNumber uint64,
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

			message := topicwriter.Message{
				Key:  messageKey,
				Data: bytes.NewReader(r.payload),
			}
			if r.writerConfig.ProducerIDPrefix != "" {
				message.SeqNo = int64(transactionNumber)
			}
			err = writer.Write(ctx, message)
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
