package transactionalwriterbenchmark

import (
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
	cfg.AutoSplitWriteSpeed = 1 << 20
	cfg.AutoSplitBurstBytes = 1 << 20
	cfg.AutoSplitUpUtilization = 2
	cfg.AutoSplitStabilization = 2 * time.Second
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
		DSN:                *standardBenchmarkDSN,
		TopicPath:          topicPath,
		TablePath:          *standardBenchmarkTable,
		RunID:              runID,
		ProducerIDPrefix:   runID,
		Mode:               benchmarkCase.Mode,
		Routing:            benchmarkCase.Routing,
		TransactionTimeout: time.Minute,
		Concurrency:        runtime.GOMAXPROCS(0),
		MessageSize:        1024,
		PreparePartitions:  partitionCount,
	}
}

func runStandardBenchmark(b *testing.B, cfg config) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(cfg.MessageSize))
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

	payload := makePayload(cfg.MessageSize)
	runners := newTransactionRunners(db, cfg, payload)
	workerStats := newFixedBenchmarkWorkerStats(cfg.Concurrency, b.N)
	streamWriteOpensBefore := metrics.streamWriteOpens.Load()
	startedAt := time.Now()
	b.ResetTimer()
	b.StartTimer()
	stats := runParallelTransactions(ctx, b, runners, workerStats)
	b.StopTimer()
	duration := time.Since(startedAt)
	streamWriteOpens := metrics.streamWriteOpens.Load() - streamWriteOpensBefore

	reportStandardBenchmarkMetrics(b, cfg, stats, duration, streamWriteOpens, nil)
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

	payload := makePayload(cfg.MessageSize)
	runners := newTransactionRunners(db, cfg, payload)
	streamWriteOpensBefore := metrics.streamWriteOpens.Load()
	b.ResetTimer()
	b.StartTimer()
	stats := runPhase(
		ctx,
		runners,
		cfg.Duration,
	)
	b.StopTimer()
	streamWriteOpens := metrics.streamWriteOpens.Load() - streamWriteOpensBefore

	describeContext, cancelDescribe := context.WithTimeout(ctx, cfg.TransactionTimeout)
	description, err = db.Topic().Describe(describeContext, cfg.TopicPath)
	cancelDescribe()
	if err != nil {
		b.Fatalf("describe final benchmark topic: %v", err)
	}
	finalTopology, err := topicTopologyFromDescription(description)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(initialTopology.ActivePartitions), "initial-active-partitions")
	reportStandardBenchmarkMetrics(b, cfg, stats, stats.Duration, streamWriteOpens, &finalTopology)
}

func runParallelTransactions(
	ctx context.Context,
	b *testing.B,
	runners []*transactionRunner,
	allStats []workerStats,
) phaseStats {
	var workerCounter atomic.Uint64
	startedAt := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		workerID := nextParallelWorkerID(&workerCounter)
		runner := runners[workerID]
		stats := &allStats[workerID]
		for pb.Next() {
			transactionNumber := stats.LogicalTransactions + 1
			stats.LogicalTransactions++
			transactionLatency, timings, attempts, err := runner.execute(ctx, transactionNumber)
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
			stats.Messages++
			stats.Bytes += uint64(len(runner.payload))
			stats.TransactionLatency = append(stats.TransactionLatency, transactionLatency)
			stats.TableLatency = append(stats.TableLatency, timings.Table)
			stats.WriterStartLatency = append(stats.WriterStartLatency, timings.WriterStart)
		}
	})

	return mergeWorkerStats(allStats, time.Since(startedAt))
}

func newFixedBenchmarkWorkerStats(workerCount, transactionCount int) []workerStats {
	allStats := make([]workerStats, workerCount)
	samplesPerWorker := (transactionCount + workerCount - 1) / workerCount
	for i := range allStats {
		allStats[i].TransactionLatency = make([]time.Duration, 0, samplesPerWorker)
		allStats[i].TableLatency = make([]time.Duration, 0, samplesPerWorker)
		allStats[i].WriterStartLatency = make([]time.Duration, 0, samplesPerWorker)
	}

	return allStats
}

func reportStandardBenchmarkMetrics(
	b *testing.B,
	cfg config,
	stats phaseStats,
	duration time.Duration,
	streamWriteOpens uint64,
	finalTopology *topicTopology,
) {
	b.Helper()
	report := stats.report()
	b.ReportMetric(report.TransactionsPerSecond, "tx/s")
	b.ReportMetric(report.Latency.Transaction.P50MS, "ms/p50")
	b.ReportMetric(report.Latency.Transaction.P95MS, "ms/p95")
	b.ReportMetric(report.Latency.Transaction.P99MS, "ms/p99")
	if report.Latency.TableExec != nil {
		b.ReportMetric(report.Latency.TableExec.P95MS, "ms/table-p95")
	}
	b.ReportMetric(report.Latency.WriterStart.P95MS, "ms/writer-start-p95")
	b.ReportMetric(float64(stats.Failed), "errors")
	b.ReportMetric(float64(stats.Failed)/float64(max(stats.LogicalTransactions, 1)), "errors/tx")
	if stats.Failed != 0 {
		b.Logf("transaction errors: %d; first error: %s", stats.Failed, stats.FirstError)
	}
	b.ReportMetric(float64(stats.Retries)/float64(max(stats.LogicalTransactions, 1)), "retries/tx")
	b.ReportMetric(
		float64(streamWriteOpens)/float64(max(stats.Committed, 1)),
		"StreamWrite/tx",
	)
	b.ReportMetric(float64(cfg.Concurrency), "workers")
	b.ReportMetric(duration.Seconds(), "measured-s")
	if finalTopology != nil {
		b.ReportMetric(float64(finalTopology.ActivePartitions), "active-partitions")
	}
}

func nextParallelWorkerID(counter *atomic.Uint64) int {
	return int(counter.Add(1) - 1)
}
