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
// Each invocation creates a unique topic and drops it on normal completion.
// It also removes its rows from the shared state table after measurement.
// Each operation has a 10-second deadline, shared across DoTx retries.
// The benchmark has no separate run deadline.

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-02, Go 1.26.4 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterSingle$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterSingle/p64-4       	      46	  25466432 ns/op	   80722 B/op	    1271 allocs/op
BenchmarkTransactionalWriterSingle/p64-4       	      45	  23763003 ns/op	   79718 B/op	    1270 allocs/op
BenchmarkTransactionalWriterSingle/p64-4       	      39	  27836536 ns/op	   80424 B/op	    1273 allocs/op
BenchmarkTransactionalWriterSingle/p128-4      	      32	  64214426 ns/op	   82066 B/op	    1284 allocs/op
BenchmarkTransactionalWriterSingle/p128-4      	      39	  34164573 ns/op	   81086 B/op	    1276 allocs/op
BenchmarkTransactionalWriterSingle/p128-4      	      50	  23628662 ns/op	   79890 B/op	    1267 allocs/op
BenchmarkTransactionalWriterSingle/p256-4      	      43	  30344337 ns/op	   80493 B/op	    1273 allocs/op
BenchmarkTransactionalWriterSingle/p256-4      	      45	  24497304 ns/op	   80199 B/op	    1273 allocs/op
BenchmarkTransactionalWriterSingle/p256-4      	      44	  23715019 ns/op	   80147 B/op	    1272 allocs/op
BenchmarkTransactionalWriterSingle/p512-4      	      42	  33689625 ns/op	   80741 B/op	    1275 allocs/op
BenchmarkTransactionalWriterSingle/p512-4      	      38	  31314998 ns/op	   80181 B/op	    1275 allocs/op
BenchmarkTransactionalWriterSingle/p512-4      	      43	  24210318 ns/op	   80296 B/op	    1272 allocs/op
*/
// BenchmarkTransactionalWriterSingle measures a single-partition writer.
func BenchmarkTransactionalWriterSingle(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "single", Mode: txWriterWriterModeSingle, Routing: txWriterRoutingModeKey,
	})
}

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-02, Go 1.26.4 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterManyKey$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterManyKey/p64-4      	       7	 167590333 ns/op	 2323629 B/op	   33731 allocs/op
BenchmarkTransactionalWriterManyKey/p64-4      	       8	 193732599 ns/op	 2286624 B/op	   33595 allocs/op
BenchmarkTransactionalWriterManyKey/p64-4      	       8	 130261031 ns/op	 2277195 B/op	   33550 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4     	       5	 300960475 ns/op	 4468344 B/op	   65788 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4     	       4	 261798323 ns/op	 4517594 B/op	   66163 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4     	       3	 570498444 ns/op	 4511770 B/op	   66137 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4     	       1	1453345416 ns/op	 8879000 B/op	  130467 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4     	       1	1056013500 ns/op	 8882776 B/op	  130598 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4     	       1	1026983375 ns/op	 8898000 B/op	  130562 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	1873451000 ns/op	17709648 B/op	  260295 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	2012563042 ns/op	17716816 B/op	  260312 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4     	       1	1692550417 ns/op	17721160 B/op	  260398 allocs/op
*/
// BenchmarkTransactionalWriterManyKey measures keyed multi-partition writing.
func BenchmarkTransactionalWriterManyKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeKey,
	})
}

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-02, Go 1.26.4 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterManyBoundedKey$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       9	 160001620 ns/op	 2280127 B/op	   33679 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       8	 137421885 ns/op	 2280066 B/op	   33678 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       7	 146103661 ns/op	 2284635 B/op	   33735 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       3	 364896278 ns/op	 4567584 B/op	   66788 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       5	 309346125 ns/op	 4526649 B/op	   66438 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       4	 286329334 ns/op	 4575468 B/op	   66857 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       2	 674821521 ns/op	 9008324 B/op	  132046 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       2	 652508896 ns/op	 9014664 B/op	  132060 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       1	1141689875 ns/op	 9002440 B/op	  131848 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	2395160084 ns/op	17960936 B/op	  263226 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1874945250 ns/op	17973016 B/op	  263281 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	2301778125 ns/op	17952104 B/op	  262982 allocs/op
*/
// BenchmarkTransactionalWriterManyBoundedKey measures bounded-key multi-partition writing.
func BenchmarkTransactionalWriterManyBoundedKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-bounded-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeBoundedKey,
	})
}

// Set the integration scope connection and credentials before running.
// Baseline: 2026-10-02, Go 1.26.4 on Apple M3 Pro (darwin/arm64), YDB
// ydb-stable-26-3-1-17 on one dedicated node with 4 CPU cores and 8 GB RAM.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterAutoSplit$' \
//	  -benchtime=300x -count=3 -cpu=4

/*
BenchmarkTransactionalWriterAutoSplit-4   	     300	  31890316 ns/op	  160822 B/op	    2502 allocs/op
BenchmarkTransactionalWriterAutoSplit-4   	     300	  30505174 ns/op	  168352 B/op	    2619 allocs/op
BenchmarkTransactionalWriterAutoSplit-4   	     300	  33228285 ns/op	  160393 B/op	    2501 allocs/op
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
