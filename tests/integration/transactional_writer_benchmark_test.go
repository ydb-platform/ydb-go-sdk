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
// Results: 2026-10-07, code base d0b3a3fa6, Go 1.27.1 on Apple M3 Pro
// (darwin/arm64), remote Yandex Cloud Managed Service for YDB.
// Server version and resources were not recorded for this run.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterSingle$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterSingle/p64-4         	      51	  22695962 ns/op	   78133 B/op	    1248 allocs/op
BenchmarkTransactionalWriterSingle/p64-4         	      49	  22007500 ns/op	   78086 B/op	    1247 allocs/op
BenchmarkTransactionalWriterSingle/p64-4         	      50	  22001617 ns/op	   78367 B/op	    1248 allocs/op
BenchmarkTransactionalWriterSingle/p128-4        	      43	  24840452 ns/op	   78171 B/op	    1247 allocs/op
BenchmarkTransactionalWriterSingle/p128-4        	      42	  23856760 ns/op	   78044 B/op	    1248 allocs/op
BenchmarkTransactionalWriterSingle/p128-4        	      46	  23442812 ns/op	   78316 B/op	    1249 allocs/op
BenchmarkTransactionalWriterSingle/p256-4        	      49	  23375645 ns/op	   78054 B/op	    1248 allocs/op
BenchmarkTransactionalWriterSingle/p256-4        	      46	  23053465 ns/op	   78359 B/op	    1249 allocs/op
BenchmarkTransactionalWriterSingle/p256-4        	      46	  25127120 ns/op	   77994 B/op	    1247 allocs/op
BenchmarkTransactionalWriterSingle/p512-4        	      46	  24225293 ns/op	   78754 B/op	    1249 allocs/op
BenchmarkTransactionalWriterSingle/p512-4        	      45	  22459359 ns/op	   78118 B/op	    1247 allocs/op
BenchmarkTransactionalWriterSingle/p512-4        	      48	  22419249 ns/op	   78385 B/op	    1250 allocs/op
*/
// BenchmarkTransactionalWriterSingle measures a single-partition writer.
func BenchmarkTransactionalWriterSingle(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "single", Mode: txWriterWriterModeSingle, Routing: txWriterRoutingModeKey,
	})
}

// Set the integration scope connection and credentials before running.
// Results: 2026-10-07, code base d0b3a3fa6, Go 1.27.1 on Apple M3 Pro
// (darwin/arm64), remote Yandex Cloud Managed Service for YDB.
// Server version and resources were not recorded for this run.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterManyKey$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterManyKey/p64-4         	       8	 148425594 ns/op	 2201030 B/op	   33026 allocs/op
BenchmarkTransactionalWriterManyKey/p64-4         	       9	 130229069 ns/op	 2189392 B/op	   32944 allocs/op
BenchmarkTransactionalWriterManyKey/p64-4         	       2	 980522146 ns/op	 2224220 B/op	   33251 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4        	       1	2044986292 ns/op	 4511464 B/op	   66025 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4        	       5	 248683808 ns/op	 4371052 B/op	   65464 allocs/op
BenchmarkTransactionalWriterManyKey/p128-4        	       5	 271558292 ns/op	 4365246 B/op	   65418 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4        	       2	 544516167 ns/op	 8731220 B/op	  130601 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4        	       2	 560156062 ns/op	 8737684 B/op	  130610 allocs/op
BenchmarkTransactionalWriterManyKey/p256-4        	       2	 559496500 ns/op	 8732792 B/op	  130597 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4        	       1	2173319333 ns/op	17878304 B/op	  262060 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4        	       1	1893252833 ns/op	17411664 B/op	  261079 allocs/op
BenchmarkTransactionalWriterManyKey/p512-4        	       1	1641541292 ns/op	17414432 B/op	  261119 allocs/op
*/
// BenchmarkTransactionalWriterManyKey measures keyed multi-partition writing.
func BenchmarkTransactionalWriterManyKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeKey,
	})
}

// Set the integration scope connection and credentials before running.
// Results: 2026-10-07, code base d0b3a3fa6, Go 1.27.1 on Apple M3 Pro
// (darwin/arm64), remote Yandex Cloud Managed Service for YDB.
// Server version and resources were not recorded for this run.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterManyBoundedKey$' -count=3 -cpu=4

/*
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       8	 135958151 ns/op	 2251727 B/op	   33452 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       8	 126586354 ns/op	 2208601 B/op	   33286 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	       8	 127357099 ns/op	 2214614 B/op	   33334 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       5	 268116500 ns/op	 4419032 B/op	   66154 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       4	 266760917 ns/op	 4428016 B/op	   66256 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	       4	 265767979 ns/op	 4430980 B/op	   66297 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       2	 568833146 ns/op	 8792240 B/op	  131742 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       2	 590984375 ns/op	 8802456 B/op	  131792 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	       2	 637427729 ns/op	 8784432 B/op	  131667 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	2384530333 ns/op	18104720 B/op	  265578 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1958072750 ns/op	17516256 B/op	  263168 allocs/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       1	1981069875 ns/op	17520584 B/op	  263256 allocs/op
*/
// BenchmarkTransactionalWriterManyBoundedKey measures bounded-key multi-partition writing.
func BenchmarkTransactionalWriterManyBoundedKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-bounded-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeBoundedKey,
	})
}

// Set the integration scope connection and credentials before running.
// Results: 2026-10-07, code base d0b3a3fa6, Go 1.27.1 on Apple M3 Pro
// (darwin/arm64), remote Yandex Cloud Managed Service for YDB.
// Server version and resources were not recorded for this run.
// Run:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterAutoSplit$' \
//	  -benchtime=300x -count=3 -cpu=4

/*
BenchmarkTransactionalWriterAutoSplit/tx-4         	     300	  29408551 ns/op	  148551 B/op	    2308 allocs/op
BenchmarkTransactionalWriterAutoSplit/tx-4         	     300	  26454153 ns/op	  151586 B/op	    2368 allocs/op
BenchmarkTransactionalWriterAutoSplit/tx-4         	     300	  27043750 ns/op	  151582 B/op	    2368 allocs/op
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
