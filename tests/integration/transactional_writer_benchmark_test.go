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
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Transactional writer benchmark: one operation is one Query transaction.
// Each operation creates a Topic writer with LazyTx, executes an UPSERT,
// writes a 1024-byte message, and commits. The Topic writer is created before
// the UPSERT so Write exercises UnLazyTX.
//
// Run against local YDB (ydbplatform/local-ydb:26.3.1.16):
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriter(Single|ManyKey|ManyBoundedKey)$' \
//	  -benchtime=10s -count=3 -cpu=4 \
//	  -args -ydb-benchmark-partitions 64,128,256,512
//
// For auto-split, use:
//
//	go test -tags integration ./tests/integration -run '^$' \
//	  -bench '^BenchmarkTransactionalWriterAutoSplit$' -benchtime=300x \
//	  -count=3 -cpu=4
//
// Connection and credentials use the integration scope environment settings.
// Each invocation uses a new topic and removes it after a successful measurement.
// Each benchmark invocation has one 20-minute deadline; transactions have no
// separate timeout. The historical results below used a one-minute deadline
// per transaction.

// BenchmarkTransactionalWriterSingle
// Master baseline measured on 2026-10-01 at aaf92e41 with Go 1.26.4 and
// ydbplatform/local-ydb:26.3.1.16. The fixed benchmark used -benchtime=10s
// -count=3 -cpu=4 and one 1024-byte message per transaction. Each attempt used
// query.WithLazyTx(true), created the Topic writer before UPSERT materialized
// the transaction, and then called Write. Every fixed run had zero final
// failures. The baseline predates the split into separate Benchmark functions
// and per-invocation topics; the old result names are retained below.
// Result:
/*
BenchmarkTransactionalWriter/p64/single-4	2200	5036600 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p64/single-4	2462	5001744 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p64/single-4	2406	5123537 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p128/single-4	1834	6530249 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p128/single-4	1737	6608322 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p128/single-4	2014	6482218 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p256/single-4	1633	7742370 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p256/single-4	1647	8465182 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p256/single-4	1683	7752079 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p512/single-4	1624	7571217 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p512/single-4	1428	8338628 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p512/single-4	1560	8429209 ns/op	0 errors	1.000 streams/tx
*/
func BenchmarkTransactionalWriterSingle(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "single", Mode: txWriterWriterModeSingle, Routing: txWriterRoutingModeKey,
	})
}

// BenchmarkTransactionalWriterManyKey
// Same master baseline and protocol as BenchmarkTransactionalWriterSingle.
// The old result names are retained because the baseline predates the split.
// Result:
/*
BenchmarkTransactionalWriter/p64/many-key-4	416	45144961 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-key-4	344	44764447 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-key-4	271	41235433 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p128/many-key-4	138	78284050 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-key-4	177	82167643 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-key-4	153	83373821 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p256/many-key-4	70	183954268 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-key-4	64	172030198 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-key-4	63	170444968 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p512/many-key-4	24	501249553 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-key-4	21	630094687 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-key-4	13	968463503 ns/op	0 errors	512.0 streams/tx
*/
func BenchmarkTransactionalWriterManyKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeKey,
	})
}

// BenchmarkTransactionalWriterManyBoundedKey
// Same master baseline and protocol as BenchmarkTransactionalWriterSingle.
// The old result names are retained because the baseline predates the split.
// Result:
/*
BenchmarkTransactionalWriter/p64/many-bounded-key-4	291	48043881 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-bounded-key-4	271	45016214 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-bounded-key-4	272	48681808 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p128/many-bounded-key-4	87	115658159 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-bounded-key-4	165	83402487 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-bounded-key-4	127	112196606 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p256/many-bounded-key-4	58	209538608 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-bounded-key-4	51	206661558 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-bounded-key-4	66	225842906 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p512/many-bounded-key-4	12	1000694106 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-bounded-key-4	12	975123006 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-bounded-key-4	14	754791876 ns/op	0 errors	512.0 streams/tx
*/
func BenchmarkTransactionalWriterManyBoundedKey(b *testing.B) {
	txWriterRunFixedPartitionBenchmark(b, txWriterStandardBenchmarkCase{
		Name: "many-bounded-key", Mode: txWriterWriterModeMany, Routing: txWriterRoutingModeBoundedKey,
	})
}

// BenchmarkTransactionalWriterAutoSplit
// Measured on 2026-10-01 at 9b19136b6 with Go 1.26.4 and
// ydbplatform/local-ydb:26.3.1.16. A single -benchtime=300x -count=3 -cpu=4
// invocation ran all three repetitions on the same YDB instance, with a
// separate topic for each repetition. Every operation is one transaction.
// A remote rerun on 2026-10-01 used YDB ydb-stable-26-3-1-17 on a dedicated
// single node with 4 cores and 8 GB RAM. Its -benchtime=300x -count=3 run did
// not finish the first repetition, so no comparable ns/op result is available.
// A separate diagnostic run showed two workers waiting for message
// acknowledgements in MultiWriter.Close before transaction commit; that topic
// had reached three active partitions. No split was awaited by the benchmark.
// Result:
/*
goos: linux
goarch: amd64
pkg: github.com/ydb-platform/ydb-go-sdk/v3/tests/integration
cpu: VirtualApple @ 2.50GHz
BenchmarkTransactionalWriterAutoSplit-4   	     300	 203306241 ns/op	         1.000 errors	         4.000 partitions	         2.759 streams/tx
BenchmarkTransactionalWriterAutoSplit-4   	     300	 201648559 ns/op	         3.000 errors	         3.000 partitions	         2.785 streams/tx
BenchmarkTransactionalWriterAutoSplit-4   	     300	 201846098 ns/op	         2.000 errors	         4.000 partitions	         2.772 streams/tx
*/
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

const txWriterBenchmarkTimeout = 20 * time.Minute

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
	// Go's -timeout alarm is stopped before benchmarks run.
	ctx, cancel := context.WithTimeout(scope.Ctx, txWriterBenchmarkTimeout)
	b.Cleanup(cancel)
	metrics := &txWriterInstrumentation{}
	db := scope.Driver(ydb.WithTraceTopic(metrics.topicTrace()))

	if err := txWriterPrepareSchema(ctx, db, cfg); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Topic().Drop(ctx, cfg.TopicPath); err != nil {
			b.Errorf("drop benchmark topic %q: %v", cfg.TopicPath, err)
		}
	})
	if cfg.AutoSplit {
		if _, err := db.Topic().Describe(ctx, cfg.TopicPath); err != nil {
			b.Fatalf("describe benchmark topic: %v", err)
		}
	}

	payload := txWriterMakePayload(cfg.MessageSize)
	runners := txWriterNewTransactionRunners(db, cfg, payload)
	workerStats := make([]txWriterWorkerStats, cfg.Concurrency)
	streamWriteOpensBefore := metrics.streamWriteOpens.Load()
	b.ResetTimer()
	b.StartTimer()
	stats := txWriterRunParallelTransactions(ctx, b, runners, workerStats)
	b.StopTimer()
	if err := ctx.Err(); err != nil {
		b.Fatalf("benchmark did not finish within %s: %v", txWriterBenchmarkTimeout, err)
	}
	streamWriteOpens := metrics.streamWriteOpens.Load() - streamWriteOpensBefore

	var finalTopology *txWriterTopicTopology
	if cfg.AutoSplit {
		// Observe the final topology without waiting for a split.
		description, err := db.Topic().Describe(ctx, cfg.TopicPath)
		if err != nil {
			b.Fatalf("describe final benchmark topic: %v", err)
		}
		topology, err := txWriterTopicTopologyFromDescription(description)
		if err != nil {
			b.Fatal(err)
		}
		finalTopology = &topology
	}
	txWriterReportStandardBenchmarkMetrics(b, stats, streamWriteOpens, finalTopology)
}

func txWriterRunParallelTransactions(
	ctx context.Context,
	b *testing.B,
	runners []*txWriterTransactionRunner,
	allStats []txWriterWorkerStats,
) txWriterWorkerStats {
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
				stats.Failed++
				if ctx.Err() != nil {
					return
				}

				continue
			}

			stats.Committed++
		}
	})

	return txWriterMergeWorkerStats(allStats)
}

func txWriterReportStandardBenchmarkMetrics(
	b *testing.B,
	stats txWriterWorkerStats,
	streamWriteOpens uint64,
	finalTopology *txWriterTopicTopology,
) {
	b.Helper()
	b.ReportMetric(float64(stats.Failed), "errors")
	b.ReportMetric(
		float64(streamWriteOpens)/float64(max(stats.Committed, 1)),
		"streams/tx",
	)
	if finalTopology != nil {
		b.ReportMetric(float64(finalTopology.ActivePartitions), "partitions")
	}
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
	Committed           uint64
	Failed              uint64
}

func txWriterMergeWorkerStats(all []txWriterWorkerStats) txWriterWorkerStats {
	var merged txWriterWorkerStats
	for i := range all {
		stats := &all[i]
		merged.LogicalTransactions += stats.LogicalTransactions
		merged.Committed += stats.Committed
		merged.Failed += stats.Failed
	}

	return merged
}

type txWriterInstrumentation struct {
	streamWriteOpens atomic.Uint64
}

func (m *txWriterInstrumentation) topicTrace() trace.Topic {
	return trace.Topic{
		OnWriterInitStream: func(trace.TopicWriterInitStreamStartInfo) func(trace.TopicWriterInitStreamDoneInfo) {
			m.streamWriteOpens.Add(1)

			return nil
		},
	}
}

type txWriterTopicTopology struct {
	ActivePartitions int
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

func txWriterTopicTopologyFromDescription(description topictypes.TopicDescription) (txWriterTopicTopology, error) {
	activePartitions := 0
	for _, partition := range description.Partitions {
		if partition.Active {
			activePartitions++
		}
	}
	if activePartitions == 0 {
		return txWriterTopicTopology{}, fmt.Errorf("topic %q has no active partitions", description.Path)
	}

	return txWriterTopicTopology{
		ActivePartitions: activePartitions,
	}, nil
}

func txWriterMakePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	return payload
}

type txWriterTransactionRunner struct {
	db                    *ydb.Driver
	topicPath             string
	tableQuery            string
	runID                 string
	workerID              int
	payload               []byte
	txWriterWriterOptions []topicoptions.WriterOption
	doTxOptions           []query.DoTxOption
	messageKeyPrefix      string
	execute               func(context.Context, uint64) error
}

func txWriterNewTransactionRunner(
	db *ydb.Driver,
	cfg txWriterConfig,
	workerID int,
	payload []byte,
) *txWriterTransactionRunner {
	runner := &txWriterTransactionRunner{
		db:                    db,
		topicPath:             cfg.TopicPath,
		tableQuery:            fmt.Sprintf(txWriterBenchmarkTableQueryTemplate, txWriterQuoteYQLPath(cfg.TablePath)),
		runID:                 cfg.RunID,
		workerID:              workerID,
		payload:               payload,
		txWriterWriterOptions: txWriterWriterOptions(cfg, workerID),
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
	return r.db.Query().DoTx(
		ctx,
		func(ctx context.Context, tx query.TxActor) error {
			writer, err := r.db.Topic().StartTransactionalWriter(
				tx,
				r.topicPath,
				r.txWriterWriterOptions...,
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
