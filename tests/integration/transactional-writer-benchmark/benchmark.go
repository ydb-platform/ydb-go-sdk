//nolint:lll // Raw Go benchmark output is intentionally kept one benchmark per line.
package transactionalwriterbenchmark

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// Master baseline measured on 2026-10-01 at aaf92e41 with Go 1.26.4 and
// ydbplatform/local-ydb:26.3.1.16. The fixed benchmark used -benchtime=10s
// -count=3 -cpu=4 and one 1024-byte message per transaction. Each attempt used
// query.WithLazyTx(true), created the Topic writer before UPSERT materialized
// the transaction, and then called Write. Every fixed run had zero retries and
// zero final failures. The raw Go benchmark output was:
/*
BenchmarkTransactionalWriter/p64/single-4                    2200      5036600 ns/op       0.20 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      11.08 measured-s      20.04 ms/p50      21.29 ms/p95      30.00 ms/p99       2.785 ms/table-p95       0.2075 ms/writer-start-p95       0 retries/tx      198.5 tx/s       4.000 workers      78094 B/op       1234 allocs/op
BenchmarkTransactionalWriter/p64/single-4                    2462      5001744 ns/op       0.20 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      12.31 measured-s      20.05 ms/p50      21.06 ms/p95      21.70 ms/p99       2.751 ms/table-p95       0.2756 ms/writer-start-p95       0 retries/tx      199.9 tx/s       4.000 workers      78098 B/op       1234 allocs/op
BenchmarkTransactionalWriter/p64/single-4                    2406      5123537 ns/op       0.20 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      12.33 measured-s      20.10 ms/p50      21.81 ms/p95      37.74 ms/p99       2.906 ms/table-p95       0.2740 ms/writer-start-p95       0 retries/tx      195.2 tx/s       4.000 workers      78136 B/op       1235 allocs/op
BenchmarkTransactionalWriter/p64/many-key-4                   416     45144961 ns/op       0.02 MB/s       64.00 StreamWrite/tx       0 errors       0 errors/tx      18.78 measured-s      180.6 ms/p50      251.8 ms/p95      279.0 ms/p99       10.82 ms/table-p95       0.5101 ms/writer-start-p95       0 retries/tx      22.15 tx/s       4.000 workers    2221079 B/op      33058 allocs/op
BenchmarkTransactionalWriter/p64/many-key-4                   344     44764447 ns/op       0.02 MB/s       64.00 StreamWrite/tx       0 errors       0 errors/tx      15.40 measured-s      178.1 ms/p50      248.3 ms/p95      269.2 ms/p99       11.17 ms/table-p95       0.6269 ms/writer-start-p95       0 retries/tx      22.34 tx/s       4.000 workers    2218939 B/op      33034 allocs/op
BenchmarkTransactionalWriter/p64/many-key-4                   271     41235433 ns/op       0.02 MB/s       64.00 StreamWrite/tx       0 errors       0 errors/tx      11.18 measured-s      160.1 ms/p50      263.0 ms/p95      316.3 ms/p99       12.59 ms/table-p95       0.3980 ms/writer-start-p95       0 retries/tx      24.25 tx/s       4.000 workers    2226272 B/op      33104 allocs/op
BenchmarkTransactionalWriter/p64/many-bounded-key-4           291     48043881 ns/op       0.02 MB/s       64.00 StreamWrite/tx       0 errors       0 errors/tx      13.98 measured-s      191.0 ms/p50      250.9 ms/p95      269.8 ms/p99       13.62 ms/table-p95       0.5155 ms/writer-start-p95       0 retries/tx      20.81 tx/s       4.000 workers    2245942 B/op      33414 allocs/op
BenchmarkTransactionalWriter/p64/many-bounded-key-4           271     45016214 ns/op       0.02 MB/s       64.00 StreamWrite/tx       0 errors       0 errors/tx      12.20 measured-s      170.6 ms/p50      274.1 ms/p95      357.8 ms/p99       13.83 ms/table-p95       0.5638 ms/writer-start-p95       0 retries/tx      22.21 tx/s       4.000 workers    2254041 B/op      33476 allocs/op
BenchmarkTransactionalWriter/p64/many-bounded-key-4           272     48681808 ns/op       0.02 MB/s       64.00 StreamWrite/tx       0 errors       0 errors/tx      13.24 measured-s      194.6 ms/p50      285.2 ms/p95      301.3 ms/p99       14.18 ms/table-p95       0.5545 ms/writer-start-p95       0 retries/tx      20.54 tx/s       4.000 workers    2249062 B/op      33435 allocs/op
BenchmarkTransactionalWriter/p128/single-4                   1834      6530249 ns/op       0.16 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      11.98 measured-s      25.96 ms/p50      38.37 ms/p95      46.50 ms/p99       5.358 ms/table-p95       0.5278 ms/writer-start-p95       0 retries/tx      153.1 tx/s       4.000 workers      78133 B/op       1237 allocs/op
BenchmarkTransactionalWriter/p128/single-4                   1737      6608322 ns/op       0.15 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      11.48 measured-s      25.78 ms/p50      38.94 ms/p95      50.82 ms/p99       5.750 ms/table-p95       0.5102 ms/writer-start-p95       0 retries/tx      151.3 tx/s       4.000 workers      78089 B/op       1237 allocs/op
BenchmarkTransactionalWriter/p128/single-4                   2014      6482218 ns/op       0.16 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      13.06 measured-s      24.00 ms/p50      37.81 ms/p95      53.63 ms/p99       5.328 ms/table-p95       0.5441 ms/writer-start-p95       0 retries/tx      154.3 tx/s       4.000 workers      78160 B/op       1237 allocs/op
BenchmarkTransactionalWriter/p128/many-key-4                  138     78284050 ns/op       0.01 MB/s       128.0 StreamWrite/tx       0 errors       0 errors/tx      10.80 measured-s      312.0 ms/p50      414.4 ms/p95      437.9 ms/p99       11.67 ms/table-p95       1.023 ms/writer-start-p95       0 retries/tx      12.77 tx/s       4.000 workers    4388542 B/op      65112 allocs/op
BenchmarkTransactionalWriter/p128/many-key-4                  177     82167643 ns/op       0.01 MB/s       128.0 StreamWrite/tx       0 errors       0 errors/tx      14.54 measured-s      324.9 ms/p50      435.9 ms/p95      473.8 ms/p99       11.63 ms/table-p95       0.4358 ms/writer-start-p95       0 retries/tx      12.17 tx/s       4.000 workers    4376240 B/op      64994 allocs/op
BenchmarkTransactionalWriter/p128/many-key-4                  153     83373821 ns/op       0.01 MB/s       128.0 StreamWrite/tx       0 errors       0 errors/tx      12.76 measured-s      328.9 ms/p50      517.3 ms/p95      544.2 ms/p99       12.78 ms/table-p95       0.8821 ms/writer-start-p95       0 retries/tx      11.99 tx/s       4.000 workers    4378949 B/op      65027 allocs/op
BenchmarkTransactionalWriter/p128/many-bounded-key-4           87    115658159 ns/op       0.01 MB/s       128.0 StreamWrite/tx       0 errors       0 errors/tx      10.06 measured-s      442.4 ms/p50      699.5 ms/p95      754.3 ms/p99       17.33 ms/table-p95       1.648 ms/writer-start-p95       0 retries/tx       8.646 tx/s       4.000 workers    4451088 B/op      65848 allocs/op
BenchmarkTransactionalWriter/p128/many-bounded-key-4          165     83402487 ns/op       0.01 MB/s       128.0 StreamWrite/tx       0 errors       0 errors/tx      13.76 measured-s      340.3 ms/p50      409.1 ms/p95      450.8 ms/p99       11.27 ms/table-p95       0.4198 ms/writer-start-p95       0 retries/tx      11.99 tx/s       4.000 workers    4438355 B/op      65709 allocs/op
BenchmarkTransactionalWriter/p128/many-bounded-key-4          127    112196606 ns/op       0.01 MB/s       128.0 StreamWrite/tx       0 errors       0 errors/tx      14.25 measured-s      414.1 ms/p50      712.8 ms/p95      757.8 ms/p99       20.75 ms/table-p95       0.4674 ms/writer-start-p95       0 retries/tx       8.913 tx/s       4.000 workers    4454508 B/op      65908 allocs/op
BenchmarkTransactionalWriter/p256/single-4                   1633      7742370 ns/op       0.13 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      12.64 measured-s      29.73 ms/p50      46.87 ms/p95      62.60 ms/p99       6.694 ms/table-p95       0.6817 ms/writer-start-p95       0 retries/tx      129.2 tx/s       4.000 workers      78054 B/op       1237 allocs/op
BenchmarkTransactionalWriter/p256/single-4                   1647      8465182 ns/op       0.12 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      13.94 measured-s      28.75 ms/p50      75.88 ms/p95      118.3 ms/p99       9.318 ms/table-p95       0.8603 ms/writer-start-p95       0 retries/tx      118.1 tx/s       4.000 workers      78040 B/op       1237 allocs/op
BenchmarkTransactionalWriter/p256/single-4                   1683      7752079 ns/op       0.13 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      13.05 measured-s      29.84 ms/p50      49.81 ms/p95      68.75 ms/p99       7.172 ms/table-p95       0.7789 ms/writer-start-p95       0 retries/tx      129.0 tx/s       4.000 workers      78064 B/op       1237 allocs/op
BenchmarkTransactionalWriter/p256/many-key-4                   70    183954268 ns/op       0.01 MB/s       256.0 StreamWrite/tx       0 errors       0 errors/tx      12.88 measured-s      722.6 ms/p50      997.2 ms/p95       1067 ms/p99       23.99 ms/table-p95       0.6419 ms/writer-start-p95       0 retries/tx       5.436 tx/s       4.000 workers    8683035 B/op     129028 allocs/op
BenchmarkTransactionalWriter/p256/many-key-4                   64    172030198 ns/op       0.01 MB/s       256.0 StreamWrite/tx       0 errors       0 errors/tx      11.01 measured-s      638.2 ms/p50       1151 ms/p95       1188 ms/p99       24.03 ms/table-p95       0.9133 ms/writer-start-p95       0 retries/tx       5.813 tx/s       4.000 workers    8726644 B/op     129409 allocs/op
BenchmarkTransactionalWriter/p256/many-key-4                   63    170444968 ns/op       0.01 MB/s       256.0 StreamWrite/tx       0 errors       0 errors/tx      10.74 measured-s      660.0 ms/p50      988.5 ms/p95       1037 ms/p99       19.81 ms/table-p95       1.098 ms/writer-start-p95       0 retries/tx       5.867 tx/s       4.000 workers    8728614 B/op     129431 allocs/op
BenchmarkTransactionalWriter/p256/many-bounded-key-4           58    209538608 ns/op       0.00 MB/s       256.0 StreamWrite/tx       0 errors       0 errors/tx      12.15 measured-s      809.7 ms/p50       1218 ms/p95       1290 ms/p99       49.13 ms/table-p95       0.4807 ms/writer-start-p95       0 retries/tx       4.772 tx/s       4.000 workers    8820197 B/op     130633 allocs/op
BenchmarkTransactionalWriter/p256/many-bounded-key-4           51    206661558 ns/op       0.00 MB/s       256.0 StreamWrite/tx       0 errors       0 errors/tx      10.54 measured-s      814.6 ms/p50       1523 ms/p95       1534 ms/p99       28.64 ms/table-p95       0.4718 ms/writer-start-p95       0 retries/tx       4.839 tx/s       4.000 workers    8787906 B/op     130336 allocs/op
BenchmarkTransactionalWriter/p256/many-bounded-key-4           66    225842906 ns/op       0.00 MB/s       256.0 StreamWrite/tx       0 errors       0 errors/tx      14.91 measured-s      824.8 ms/p50       1505 ms/p95       1621 ms/p99       46.74 ms/table-p95       0.3048 ms/writer-start-p95       0 retries/tx       4.428 tx/s       4.000 workers    8828510 B/op     130721 allocs/op
BenchmarkTransactionalWriter/p512/single-4                   1624      7571217 ns/op       0.14 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      12.30 measured-s      29.41 ms/p50      47.59 ms/p95      61.04 ms/p99       7.165 ms/table-p95       0.7174 ms/writer-start-p95       0 retries/tx      132.1 tx/s       4.000 workers      77967 B/op       1236 allocs/op
BenchmarkTransactionalWriter/p512/single-4                   1428      8338628 ns/op       0.12 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      11.91 measured-s      31.37 ms/p50      55.69 ms/p95      69.93 ms/p99       7.855 ms/table-p95       0.6351 ms/writer-start-p95       0 retries/tx      119.9 tx/s       4.000 workers      77937 B/op       1236 allocs/op
BenchmarkTransactionalWriter/p512/single-4                   1560      8429209 ns/op       0.12 MB/s       1.000 StreamWrite/tx       0 errors       0 errors/tx      13.15 measured-s      31.41 ms/p50      51.89 ms/p95      84.39 ms/p99       8.098 ms/table-p95       0.7126 ms/writer-start-p95       0 retries/tx      118.6 tx/s       4.000 workers      77968 B/op       1237 allocs/op
BenchmarkTransactionalWriter/p512/many-key-4                   24    501249553 ns/op       0.00 MB/s       512.0 StreamWrite/tx       0 errors       0 errors/tx      12.03 measured-s       2047 ms/p50       2351 ms/p95       2353 ms/p99       28.76 ms/table-p95       0.6105 ms/writer-start-p95       0 retries/tx       1.995 tx/s       4.000 workers   17371899 B/op     257991 allocs/op
BenchmarkTransactionalWriter/p512/many-key-4                   21    630094687 ns/op       0.00 MB/s       512.0 StreamWrite/tx       0 errors       0 errors/tx      13.23 measured-s       1895 ms/p50       3339 ms/p95       3339 ms/p99       62.66 ms/table-p95       0.8040 ms/writer-start-p95       0 retries/tx       1.587 tx/s       4.000 workers   17398086 B/op     258217 allocs/op
BenchmarkTransactionalWriter/p512/many-key-4                   13    968463503 ns/op       0.00 MB/s       512.0 StreamWrite/tx       0 errors       0 errors/tx      12.59 measured-s       3447 ms/p50       4809 ms/p95       4809 ms/p99       127.1 ms/table-p95       1.570 ms/writer-start-p95       0 retries/tx       1.033 tx/s       4.000 workers   17361902 B/op     257897 allocs/op
BenchmarkTransactionalWriter/p512/many-bounded-key-4           12   1000694106 ns/op       0.00 MB/s       512.0 StreamWrite/tx       0 errors       0 errors/tx      12.01 measured-s       4236 ms/p50       4330 ms/p95       4330 ms/p99       38.48 ms/table-p95       0.9289 ms/writer-start-p95       0 retries/tx      0.9993 tx/s       4.000 workers   17755273 B/op     262297 allocs/op
BenchmarkTransactionalWriter/p512/many-bounded-key-4           12    975123006 ns/op       0.00 MB/s       512.0 StreamWrite/tx       0 errors       0 errors/tx      11.70 measured-s       4105 ms/p50       4428 ms/p95       4428 ms/p99       77.83 ms/table-p95       1.817 ms/writer-start-p95       0 retries/tx       1.026 tx/s       4.000 workers   17828915 B/op     262992 allocs/op
BenchmarkTransactionalWriter/p512/many-bounded-key-4           14    754791876 ns/op       0.00 MB/s       512.0 StreamWrite/tx       0 errors       0 errors/tx      10.57 measured-s       2798 ms/p50       3636 ms/p95       3636 ms/p99       324.3 ms/table-p95       0.8279 ms/writer-start-p95       0 retries/tx       1.325 tx/s       4.000 workers   17735246 B/op     262039 allocs/op
*/
//
// The auto-split benchmark used -benchtime=1x -count=1 -cpu=4 three times. One
// operation is a two-minute phase, and every repetition used a fresh YDB
// container. Its raw output was:
/*
BenchmarkTransactionalWriterAutoSplit-4     1   179157461084 ns/op       5.967 StreamWrite/tx       7.000 active-partitions       8.000 errors       0.002724 errors/tx       1.000 initial-active-partitions       179.2 measured-s      38.02 ms/p50      67.99 ms/p95      111.6 ms/p99       7.765 ms/table-p95       0.3597 ms/writer-start-p95       0.002043 retries/tx      16.35 tx/s       4.000 workers     788206528 B/op   12055022 allocs/op
BenchmarkTransactionalWriterAutoSplit-4     1   121548075853 ns/op       4.940 StreamWrite/tx       7.000 active-partitions       6.000 errors       0.001390 errors/tx       1.000 initial-active-partitions       121.5 measured-s      25.12 ms/p50      42.74 ms/p95      62.96 ms/p99       5.307 ms/table-p95       0.2154 ms/writer-start-p95       0.001854 retries/tx      35.45 tx/s       4.000 workers    1011723712 B/op   15498739 allocs/op
BenchmarkTransactionalWriterAutoSplit-4     1   150467076780 ns/op       6.779 StreamWrite/tx       7.000 active-partitions       7.000 errors       0.002297 errors/tx       1.000 initial-active-partitions       150.5 measured-s      49.37 ms/p50      99.48 ms/p95      142.1 ms/p99       12.02 ms/table-p95       0.4736 ms/writer-start-p95       0.002953 retries/tx      20.21 tx/s       4.000 workers     899520544 B/op   13742934 allocs/op
*/
//
// Preserve this protocol and environment when comparing a candidate change.

const benchmarkTableQueryTemplate = `
UPSERT INTO %s (run_id, worker_id, updated_at)
VALUES ($run_id, $worker_id, CurrentUtcTimestamp());
`

const ydbMaxTopicPartitions int64 = 35_000

func topicAutoPartitioningSettings(cfg config) topictypes.AutoPartitioningSettings {
	settings := topictypes.AutoPartitioningSettings{
		AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyDisabled,
	}
	if cfg.Routing == routingModeBoundedKey {
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

type attemptTimings struct {
	Table       time.Duration
	WriterStart time.Duration
}

func prepareSchema(ctx context.Context, db *ydb.Driver, cfg config) error {
	statement := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    run_id Utf8 NOT NULL,
    worker_id Uint64 NOT NULL,
    updated_at Timestamp,
    PRIMARY KEY (run_id, worker_id)
);
`, quoteYQLPath(cfg.TablePath))
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
		topicoptions.CreateWithAutoPartitioningSettings(topicAutoPartitioningSettings(cfg)),
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

func topicTopologyFromDescription(description topictypes.TopicDescription) (topicTopology, error) {
	activePartitions := 0
	for _, partition := range description.Partitions {
		if partition.Active {
			activePartitions++
		}
	}
	if activePartitions == 0 {
		return topicTopology{}, fmt.Errorf("topic %q has no active partitions", description.Path)
	}

	return topicTopology{
		ActivePartitions: activePartitions,
	}, nil
}

func makePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	return payload
}

func runPhase(
	parent context.Context,
	runners []*transactionRunner,
	duration time.Duration,
) phaseStats {
	phaseContext, cancelPhase := context.WithTimeout(parent, duration)
	defer cancelPhase()

	results := make(chan workerStats, len(runners))
	startedAt := time.Now()

	var workers sync.WaitGroup
	workers.Add(len(runners))
	for _, runner := range runners {
		go func() {
			defer workers.Done()
			results <- runWorker(
				parent,
				phaseContext.Done(),
				runner,
			)
		}()
	}

	<-phaseContext.Done()
	workers.Wait()
	endedAt := time.Now()
	close(results)

	perWorker := make([]workerStats, 0, len(runners))
	for result := range results {
		perWorker = append(perWorker, result)
	}

	return mergeWorkerStats(perWorker, endedAt.Sub(startedAt))
}

func runWorker(
	ctx context.Context,
	phaseDone <-chan struct{},
	runner *transactionRunner,
) workerStats {
	var stats workerStats

	for {
		select {
		case <-phaseDone:
			return stats
		default:
		}

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
}

type transactionRunner struct {
	db                 *ydb.Driver
	topicPath          string
	tableQuery         string
	runID              string
	workerID           int
	payload            []byte
	transactionTimeout time.Duration
	writerOptions      []topicoptions.WriterOption
	doTxOptions        []query.DoTxOption
	messageKeyPrefix   string
	execute            func(context.Context, uint64) (time.Duration, attemptTimings, int, error)
}

func newTransactionRunner(db *ydb.Driver, cfg config, workerID int, payload []byte) *transactionRunner {
	runner := &transactionRunner{
		db:                 db,
		topicPath:          cfg.TopicPath,
		tableQuery:         fmt.Sprintf(benchmarkTableQueryTemplate, quoteYQLPath(cfg.TablePath)),
		runID:              cfg.RunID,
		workerID:           workerID,
		payload:            payload,
		transactionTimeout: cfg.TransactionTimeout,
		writerOptions:      writerOptions(cfg, workerID),
		doTxOptions: []query.DoTxOption{
			query.WithIdempotent(),
			query.WithLazyTx(true),
		},
	}
	if cfg.Mode == writerModeMany {
		runner.messageKeyPrefix = fmt.Sprintf("worker-%d-message-", workerID)
		runner.execute = runner.executeManyWriterTransaction
	} else {
		runner.execute = runner.executeSingleWriterTransaction
	}

	return runner
}

func newTransactionRunners(db *ydb.Driver, cfg config, payload []byte) []*transactionRunner {
	runners := make([]*transactionRunner, cfg.Concurrency)
	for workerID := range runners {
		runners[workerID] = newTransactionRunner(db, cfg, workerID, payload)
	}

	return runners
}

func (r *transactionRunner) executeSingleWriterTransaction(
	ctx context.Context,
	_ uint64,
) (time.Duration, attemptTimings, int, error) {
	return r.executeTransaction(ctx, "")
}

func (r *transactionRunner) executeManyWriterTransaction(
	ctx context.Context,
	transactionNumber uint64,
) (time.Duration, attemptTimings, int, error) {
	return r.executeTransaction(ctx, r.messageKey(transactionNumber))
}

func (r *transactionRunner) messageKey(transactionNumber uint64) string {
	return r.messageKeyPrefix + strconv.FormatUint(transactionNumber, 10)
}

//nolint:funlen // The complete measured transaction is intentionally kept together.
func (r *transactionRunner) executeTransaction(
	parent context.Context,
	messageKey string,
) (time.Duration, attemptTimings, int, error) {
	transactionContext, cancel := context.WithTimeout(parent, r.transactionTimeout)
	defer cancel()

	var (
		attempts    int
		lastTimings attemptTimings
	)
	startedAt := time.Now()
	err := r.db.Query().DoTx(
		transactionContext,
		func(ctx context.Context, tx query.TxActor) error {
			attempts++
			currentTimings := attemptTimings{}

			writerStartedAt := time.Now()
			writer, err := r.db.Topic().StartTransactionalWriter(
				tx,
				r.topicPath,
				r.writerOptions...,
			)
			currentTimings.WriterStart = time.Since(writerStartedAt)
			if err != nil {
				lastTimings = currentTimings

				return fmt.Errorf("start transactional writer: %w", err)
			}

			tableStartedAt := time.Now()
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
			currentTimings.Table = time.Since(tableStartedAt)
			if err != nil {
				lastTimings = currentTimings

				return fmt.Errorf("execute benchmark table upsert: %w", err)
			}

			err = writer.Write(ctx, topicwriter.Message{
				Key:  messageKey,
				Data: bytes.NewReader(r.payload),
			})
			lastTimings = currentTimings
			if err != nil {
				return fmt.Errorf("write transactional topic messages: %w", err)
			}

			return nil
		},
		r.doTxOptions...,
	)

	return time.Since(startedAt), lastTimings, attempts, err
}

func writerOptions(cfg config, workerID int) []topicoptions.WriterOption {
	options := []topicoptions.WriterOption{
		topicoptions.WithWriterDirectWrite(false),
	}
	slotProducerID := ""
	if cfg.ProducerIDPrefix != "" {
		slotProducerID = fmt.Sprintf("%s-slot-%d", cfg.ProducerIDPrefix, workerID)
	}

	if cfg.Mode == writerModeSingle {
		if slotProducerID != "" {
			options = append(options, topicoptions.WithWriterProducerID(slotProducerID))
		}

		return options
	}

	multiWriterOptions := []topicoptions.MultiWriterOption{
		topicoptions.WithMultiWriterDirectWrite(false),
	}
	switch cfg.Routing {
	case routingModeKey:
		multiWriterOptions = append(
			multiWriterOptions,
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
		)
	case routingModeBoundedKey:
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

func quoteYQLPath(path string) string {
	return "`" + path + "`"
}
