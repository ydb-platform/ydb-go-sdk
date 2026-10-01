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
		b.Run(fmt.Sprintf("p%d", partitionCount), func(b *testing.B) {
			for _, benchmarkCase := range standardBenchmarkCases {
				b.Run(benchmarkCase.Name, func(b *testing.B) {
					cfg := newStandardBenchmarkConfig(
						fmt.Sprintf("%s-%s-p%d", *standardBenchmarkTopicPrefix, benchmarkCase.Name, partitionCount),
						benchmarkCase,
						partitionCount,
					)
					runStandardBenchmark(b, cfg)
				})
			}
		})
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
	workerStats := make([]workerStats, cfg.Concurrency)
	streamWriteOpensBefore := metrics.streamWriteOpens.Load()
	b.ResetTimer()
	b.StartTimer()
	stats := runParallelTransactions(ctx, b, runners, workerStats)
	b.StopTimer()
	streamWriteOpens := metrics.streamWriteOpens.Load() - streamWriteOpensBefore

	reportStandardBenchmarkMetrics(b, stats, streamWriteOpens, nil)
}

func runStandardAutoSplitBenchmark(b *testing.B, cfg config) {
	b.Helper()
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
	_, err = db.Topic().Describe(ctx, cfg.TopicPath)
	if err != nil {
		b.Fatalf("describe benchmark topic: %v", err)
	}

	payload := makePayload(cfg.MessageSize)
	runners := newTransactionRunners(db, cfg, payload)
	workerStats := make([]workerStats, cfg.Concurrency)
	streamWriteOpensBefore := metrics.streamWriteOpens.Load()
	b.ResetTimer()
	b.StartTimer()
	stats := runParallelTransactions(ctx, b, runners, workerStats)
	b.StopTimer()
	streamWriteOpens := metrics.streamWriteOpens.Load() - streamWriteOpensBefore

	describeContext, cancelDescribe := context.WithTimeout(ctx, cfg.TransactionTimeout)
	description, err := db.Topic().Describe(describeContext, cfg.TopicPath)
	cancelDescribe()
	if err != nil {
		b.Fatalf("describe final benchmark topic: %v", err)
	}
	finalTopology, err := topicTopologyFromDescription(description)
	if err != nil {
		b.Fatal(err)
	}
	reportStandardBenchmarkMetrics(b, stats, streamWriteOpens, &finalTopology)
}

func runParallelTransactions(
	ctx context.Context,
	b *testing.B,
	runners []*transactionRunner,
	allStats []workerStats,
) phaseStats {
	var workerCounter atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := nextParallelWorkerID(&workerCounter)
		runner := runners[workerID]
		stats := &allStats[workerID]
		for pb.Next() {
			transactionNumber := stats.LogicalTransactions + 1
			stats.LogicalTransactions++
			err := runner.execute(ctx, transactionNumber)
			if err != nil {
				stats.Failed++

				continue
			}

			stats.Committed++
		}
	})

	return mergeWorkerStats(allStats)
}

func reportStandardBenchmarkMetrics(
	b *testing.B,
	stats phaseStats,
	streamWriteOpens uint64,
	finalTopology *topicTopology,
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

func nextParallelWorkerID(counter *atomic.Uint64) int {
	return int(counter.Add(1) - 1)
}
