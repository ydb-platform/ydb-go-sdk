package transactionalwriterbenchmark

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	standardBenchmarkDSN = flag.String(
		"ydb-benchmark-dsn",
		benchmarkDSNFromEnvironment(),
		"YDB connection string used by transactional writer benchmarks",
	)
	standardBenchmarkTopicPrefix = flag.String(
		"ydb-benchmark-topic-prefix",
		"tx-writer-benchmark",
		"topic name prefix used by transactional writer benchmarks",
	)
	standardBenchmarkTable = flag.String(
		"ydb-benchmark-table",
		"tx-writer-benchmark-state",
		"table used by transactional writer benchmarks",
	)
	standardBenchmarkPartitions = flag.String(
		"ydb-benchmark-partitions",
		"64,128,256,512",
		"comma-separated fixed partition counts",
	)
)

type standardBenchmarkCase struct {
	Name    string
	Mode    writerMode
	Routing routingMode
}

var standardBenchmarkCases = []standardBenchmarkCase{
	{Name: "single", Mode: writerModeSingle, Routing: routingModeKey},
	{Name: "many-key", Mode: writerModeMany, Routing: routingModeKey},
	{Name: "many-bounded-key", Mode: writerModeMany, Routing: routingModeBoundedKey},
	{Name: "many-partition-id", Mode: writerModeMany, Routing: routingModePartitionID},
}

func BenchmarkTransactionalWriter(b *testing.B) {
	partitions, err := parseBenchmarkPartitions(*standardBenchmarkPartitions)
	if err != nil {
		b.Fatal(err)
	}

	for _, partitionCount := range partitions {
		for _, benchmarkCase := range standardBenchmarkCases {
			b.Run(fmt.Sprintf("%s/p%d", benchmarkCase.Name, partitionCount), func(b *testing.B) {
				cfg := newStandardBenchmarkConfig(
					fmt.Sprintf("%s-%s-p%d", *standardBenchmarkTopicPrefix, benchmarkCase.Name, partitionCount),
					benchmarkCase,
					partitionCount,
				)
				runStandardBenchmark(b, cfg)
			})
		}
	}
}

func BenchmarkTransactionalWriterAutoSplit(b *testing.B) {
	benchmarkCase := standardBenchmarkCase{
		Name:    "many-bounded-autosplit",
		Mode:    writerModeMany,
		Routing: routingModeBoundedKey,
	}
	topicPath := fmt.Sprintf("%s-autosplit-%s", *standardBenchmarkTopicPrefix, defaultRunID())
	cfg := newStandardBenchmarkConfig(topicPath, benchmarkCase, 1)
	cfg.AutoSplit = true
	cfg.AutoSplitMaxPartitions = 64
	cfg.AutoSplitWriteSpeed = 1 << 20
	cfg.AutoSplitBurstBytes = 1 << 20
	cfg.AutoSplitUpUtilization = 2
	cfg.AutoSplitStabilization = 2 * time.Second
	cfg.AutoSplitPollInterval = 250 * time.Millisecond
	cfg.Duration = 2 * time.Minute

	runStandardAutoSplitBenchmark(b, cfg)
}

func benchmarkDSNFromEnvironment() string {
	if dsn := os.Getenv("YDB_CONNECTION_STRING"); dsn != "" {
		return dsn
	}

	return "grpc://localhost:2136/local"
}

func parseBenchmarkPartitions(value string) ([]int64, error) {
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

func newStandardBenchmarkConfig(
	topicPath string,
	benchmarkCase standardBenchmarkCase,
	partitionCount int64,
) config {
	runID := defaultRunID()

	return config{
		DSN:                    *standardBenchmarkDSN,
		TopicPath:              topicPath,
		TablePath:              *standardBenchmarkTable,
		RunID:                  runID,
		ProducerIDPrefix:       runID,
		Mode:                   benchmarkCase.Mode,
		Routing:                benchmarkCase.Routing,
		AutoSeqNo:              true,
		QueryRetries:           true,
		TransactionTimeout:     time.Minute,
		Concurrency:            runtime.GOMAXPROCS(0),
		MessagesPerTx:          1,
		MessageSize:            1024,
		LatencySampleEvery:     1,
		MaxErrors:              100,
		PreparePartitions:      partitionCount,
		AutoSplitMaxPartitions: partitionCount,
	}
}

func runStandardBenchmark(b *testing.B, cfg config) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(cfg.MessagesPerTx * cfg.MessageSize))
	b.StopTimer()

	ctx := context.Background()
	metrics := &instrumentation{}
	db, err := openDatabase(ctx, cfg, metrics)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClose()
		if closeErr := db.Close(closeContext); closeErr != nil {
			b.Errorf("close benchmark driver: %v", closeErr)
		}
	}()

	if err = prepareSchema(ctx, db, cfg); err != nil {
		b.Fatal(err)
	}
	description, err := db.Topic().Describe(ctx, cfg.TopicPath)
	if err != nil {
		b.Fatalf("describe benchmark topic: %v", err)
	}
	initialTopology, err := topicTopologyFromDescription(description)
	if err != nil {
		b.Fatal(err)
	}

	payload := makePayload(cfg.MessageSize)
	lifecycleBefore := metrics.snapshot()
	startedAt := time.Now()
	b.ResetTimer()
	b.StartTimer()
	stats := runParallelTransactions(ctx, b, db, cfg, initialTopology.ActivePartitionIDs, payload)
	b.StopTimer()
	duration := time.Since(startedAt)
	lifecycle := metrics.snapshot().subtract(lifecycleBefore)

	reportStandardBenchmarkMetrics(b, cfg, stats, duration, lifecycle, nil)
	if stats.Failed != 0 {
		b.Fatalf("%d transactions failed; first error: %s", stats.Failed, stats.FirstError)
	}
}

func runStandardAutoSplitBenchmark(b *testing.B, cfg config) {
	b.Helper()
	if b.N != 1 {
		b.Fatalf("auto-split benchmark requires -benchtime=1x; got b.N=%d", b.N)
	}
	b.ReportAllocs()
	b.StopTimer()

	ctx := context.Background()
	metrics := &instrumentation{}
	db, err := openDatabase(ctx, cfg, metrics)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClose()
		if closeErr := db.Close(closeContext); closeErr != nil {
			b.Errorf("close benchmark driver: %v", closeErr)
		}
	}()

	if err = prepareSchema(ctx, db, cfg); err != nil {
		b.Fatal(err)
	}
	description, err := db.Topic().Describe(ctx, cfg.TopicPath)
	if err != nil {
		b.Fatalf("describe benchmark topic: %v", err)
	}
	initialTopology, err := topicTopologyFromDescription(description)
	if err != nil {
		b.Fatal(err)
	}
	recorder, stopTopology := startBenchmarkTopologyMonitor(ctx, b, cfg, initialTopology)

	payload := makePayload(cfg.MessageSize)
	sequences := make([]atomic.Uint64, cfg.Concurrency)
	lifecycleBefore := metrics.snapshot()
	b.ResetTimer()
	b.StartTimer()
	stats, measurementErr := runPhase(
		ctx,
		db,
		cfg,
		initialTopology.ActivePartitionIDs,
		payload,
		cfg.Duration,
		sequences,
	)
	b.StopTimer()
	lifecycle := metrics.snapshot().subtract(lifecycleBefore)

	if err = stopTopology(); err != nil {
		b.Errorf("stop topology monitor: %v", err)
	}
	reportStandardBenchmarkMetrics(b, cfg, stats, stats.Duration, lifecycle, recorder)
	if measurementErr != nil {
		b.Fatalf("run auto-split phase: %v", measurementErr)
	}
	if stats.Failed != 0 {
		b.Fatalf("%d transactions failed; first error: %s", stats.Failed, stats.FirstError)
	}
}

func runParallelTransactions(
	ctx context.Context,
	b *testing.B,
	db *ydb.Driver,
	cfg config,
	activePartitionIDs []int64,
	payload []byte,
) phaseStats {
	var (
		workerCounter atomic.Uint64
		statsMu       sync.Mutex
		allStats      []workerStats
	)
	startedAt := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		workerID := nextParallelWorkerID(&workerCounter)
		var stats workerStats
		for pb.Next() {
			logicalSequence := stats.LogicalTransactions + 1
			stats.LogicalTransactions++
			transactionLatency, timings, attempts, err := executeTransaction(
				ctx,
				db,
				cfg,
				workerID,
				logicalSequence,
				activePartitionIDs,
				payload,
			)
			stats.Attempts += uint64(attempts)
			if attempts > 1 {
				stats.Retries += uint64(attempts - 1)
			}
			if err != nil {
				stats.Failed++
				if stats.FirstError == "" {
					stats.FirstError = err.Error()
				}

				continue
			}

			stats.Committed++
			stats.Messages += uint64(cfg.MessagesPerTx)
			stats.Bytes += uint64(cfg.MessagesPerTx) * uint64(len(payload))
			if logicalSequence%uint64(cfg.LatencySampleEvery) == 0 {
				stats.TransactionLatency = append(stats.TransactionLatency, transactionLatency)
				if !cfg.SkipTableWrite {
					stats.TableLatency = append(stats.TableLatency, timings.Table)
				}
				stats.WriterStartLatency = append(stats.WriterStartLatency, timings.WriterStart)
				stats.WriterWriteLatency = append(stats.WriterWriteLatency, timings.WriterWrite)
			}
		}
		appendParallelStats(&statsMu, &allStats, stats)
	})

	return mergeParallelStats(allStats, time.Since(startedAt))
}

func startBenchmarkTopologyMonitor(
	ctx context.Context,
	b *testing.B,
	cfg config,
	initialTopology topicTopology,
) (*topologyRecorder, func() error) {
	b.Helper()
	monitorDB, err := openDatabase(ctx, cfg, nil)
	if err != nil {
		b.Fatalf("open topology monitor driver: %v", err)
	}
	recorder := newTopologyRecorder(time.Now(), initialTopology)
	monitorContext, cancelMonitor := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		monitorTopicTopology(monitorContext, monitorDB, cfg.TopicPath, cfg.AutoSplitPollInterval, recorder)
	}()

	return recorder, func() error {
		return finishTopologyMonitor(ctx, cfg, monitorDB, recorder, cancelMonitor, monitorDone)
	}
}

func reportStandardBenchmarkMetrics(
	b *testing.B,
	cfg config,
	stats phaseStats,
	duration time.Duration,
	lifecycle instrumentationSnapshot,
	recorder *topologyRecorder,
) {
	b.Helper()
	report := stats.report(cfg.SkipTableWrite)
	b.ReportMetric(report.TransactionsPerSecond, "tx/s")
	b.ReportMetric(report.Latency.Transaction.P50MS, "ms/p50")
	b.ReportMetric(report.Latency.Transaction.P95MS, "ms/p95")
	b.ReportMetric(report.Latency.Transaction.P99MS, "ms/p99")
	if report.Latency.TableExec != nil {
		b.ReportMetric(report.Latency.TableExec.P95MS, "ms/table-p95")
	}
	b.ReportMetric(report.Latency.WriterStart.P95MS, "ms/writer-start-p95")
	b.ReportMetric(report.Latency.WriterWrite.P95MS, "ms/writer-write-p95")
	b.ReportMetric(float64(stats.Retries)/float64(max(stats.LogicalTransactions, 1)), "retries/tx")
	b.ReportMetric(
		float64(lifecycle.StreamWriteOpens)/float64(max(stats.Committed, 1)),
		"StreamWrite/tx",
	)
	b.ReportMetric(float64(cfg.Concurrency), "workers")
	b.ReportMetric(duration.Seconds(), "measured-s")
	if recorder != nil {
		topology := recorder.snapshot()
		b.ReportMetric(float64(len(topology.Final.ActivePartitionIDs)), "active-partitions")
		b.ReportMetric(float64(topology.FirstSplitAfter)/float64(time.Millisecond), "ms/first-split")
	}
}

func mergeParallelStats(all []workerStats, duration time.Duration) phaseStats {
	return mergeWorkerStats(all, duration, false)
}

func appendParallelStats(mu *sync.Mutex, all *[]workerStats, stats workerStats) {
	mu.Lock()
	defer mu.Unlock()
	*all = append(*all, stats)
}

func nextParallelWorkerID(counter *atomic.Uint64) int {
	return int(counter.Add(1) - 1)
}
