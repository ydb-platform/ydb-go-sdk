//go:build integration

package integration

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
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
//	  -benchtime=10s -count=3 -cpu=4 \
//	  -args -ydb-benchmark-partitions 64,128,256,512
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
// Baseline measured on 2026-10-02 with Go 1.26.4 on Apple M3 Pro
// (darwin/arm64), against YDB ydb-stable-26-3-1-17 on one dedicated node
// with 4 CPU cores and 8 GB RAM. Each transaction had a 10-second deadline.
// The fixed and auto-split commands above each completed with PASS; all
// 39 rows below are unmodified benchmark output with no failed operations.
/*
goos: darwin
goarch: arm64
pkg: github.com/ydb-platform/ydb-go-sdk/v3/tests/integration
cpu: Apple M3 Pro
BenchmarkTransactionalWriterSingle/p64-4       	     355	  32497124 ns/op
BenchmarkTransactionalWriterSingle/p64-4       	     367	  32464069 ns/op
BenchmarkTransactionalWriterSingle/p64-4       	     351	  33439357 ns/op
BenchmarkTransactionalWriterSingle/p128-4      	     367	  33148925 ns/op
BenchmarkTransactionalWriterSingle/p128-4      	     363	  32243353 ns/op
BenchmarkTransactionalWriterSingle/p128-4      	     351	  32412980 ns/op
BenchmarkTransactionalWriterSingle/p256-4      	     342	  33911433 ns/op
BenchmarkTransactionalWriterSingle/p256-4      	     369	  33088799 ns/op
BenchmarkTransactionalWriterSingle/p256-4      	     364	  33357379 ns/op
BenchmarkTransactionalWriterSingle/p512-4      	     356	  32997020 ns/op
BenchmarkTransactionalWriterSingle/p512-4      	     355	  34146597 ns/op
BenchmarkTransactionalWriterSingle/p512-4      	     331	  33937414 ns/op
BenchmarkTransactionalWriterManyKey/p64-4      	      75	 144769740 ns/op
BenchmarkTransactionalWriterManyKey/p64-4      	      80	 139769938 ns/op
BenchmarkTransactionalWriterManyKey/p64-4      	      78	 156178657 ns/op
BenchmarkTransactionalWriterManyKey/p128-4     	      46	 240760663 ns/op
BenchmarkTransactionalWriterManyKey/p128-4     	      48	 254393774 ns/op
BenchmarkTransactionalWriterManyKey/p128-4     	      46	 259458513 ns/op
BenchmarkTransactionalWriterManyKey/p256-4     	      24	 451599054 ns/op
BenchmarkTransactionalWriterManyKey/p256-4     	      24	 449597504 ns/op
BenchmarkTransactionalWriterManyKey/p256-4     	      22	 528529062 ns/op
BenchmarkTransactionalWriterManyKey/p512-4     	      10	1585128883 ns/op
BenchmarkTransactionalWriterManyKey/p512-4     	      10	1139400546 ns/op
BenchmarkTransactionalWriterManyKey/p512-4     	      12	1079705983 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	      76	 155905867 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	      75	 148476898 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p64-4         	      90	 135516182 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	      40	 267564233 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	      42	 287812634 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p128-4        	      39	 312839130 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	      20	 583097671 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	      20	 570857850 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p256-4        	      20	 543307648 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	      10	1007578271 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	      12	 953287396 ns/op
BenchmarkTransactionalWriterManyBoundedKey/p512-4        	       9	1146171921 ns/op
BenchmarkTransactionalWriterAutoSplit-4   	     300	  43111233 ns/op
BenchmarkTransactionalWriterAutoSplit-4   	     300	  42265395 ns/op
BenchmarkTransactionalWriterAutoSplit-4   	     300	  42511449 ns/op
*/

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

// BenchmarkTransactionalWriterAutoSplit measures bounded-key writing while YDB may split partitions.
func BenchmarkTransactionalWriterAutoSplit(b *testing.B) {
	benchmarkCase := txWriterStandardBenchmarkCase{
		Name:    "many-bounded-autosplit",
		Mode:    txWriterWriterModeMany,
		Routing: txWriterRoutingModeBoundedKey,
	}
	topicPath := fmt.Sprintf("%s-autosplit", *txWriterStandardBenchmarkTopicPrefix)
	cfg := txWriterNewStandardBenchmarkConfig(topicPath, benchmarkCase, 1)
	cfg.AutoSplit = true
	cfg.AutoSplitWriteSpeed = 1 << 20
	cfg.AutoSplitBurstBytes = 1 << 20
	cfg.AutoSplitUpUtilization = 2
	cfg.AutoSplitStabilization = 2 * time.Second

	txWriterRunBenchmark(b, cfg)
}

var (
	txWriterStandardBenchmarkTopicPrefix = flag.String(
		"ydb-benchmark-topic-prefix",
		"tx-writer-benchmark",
		"topic name prefix used by transactional writer benchmarks",
	)
	txWriterStandardBenchmarkTable = flag.String(
		"ydb-benchmark-table",
		"tx-writer-benchmark-state",
		"table used by transactional writer benchmarks",
	)
	txWriterStandardBenchmarkPartitions = flag.String(
		"ydb-benchmark-partitions",
		"64,128,256,512",
		"comma-separated fixed partition counts",
	)
)

const txWriterTransactionTimeout = 10 * time.Second

type txWriterStandardBenchmarkCase struct {
	Name    string
	Mode    txWriterWriterMode
	Routing txWriterRoutingMode
}

func txWriterParseBenchmarkPartitions(value string) ([]int64, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("partition list is empty")
	}

	parts := strings.Split(value, ",")
	partitions := make([]int64, 0, len(parts))
	for _, part := range parts {
		partitionCount, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || partitionCount <= 0 {
			return nil, fmt.Errorf("invalid partition count %q", part)
		}
		partitions = append(partitions, partitionCount)
	}

	return partitions, nil
}

func txWriterNewStandardBenchmarkConfig(
	topicPath string,
	benchmarkCase txWriterStandardBenchmarkCase,
	partitionCount int64,
) txWriterConfig {
	runID := txWriterDefaultRunID()

	return txWriterConfig{
		TopicPath:         topicPath + "-" + runID,
		TablePath:         *txWriterStandardBenchmarkTable,
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
	partitions, err := txWriterParseBenchmarkPartitions(*txWriterStandardBenchmarkPartitions)
	if err != nil {
		b.Fatal(err)
	}

	for _, partitionCount := range partitions {
		b.Run(fmt.Sprintf("p%d", partitionCount), func(b *testing.B) {
			cfg := txWriterNewStandardBenchmarkConfig(
				fmt.Sprintf("%s-%s-p%d", *txWriterStandardBenchmarkTopicPrefix, benchmarkCase.Name, partitionCount),
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
