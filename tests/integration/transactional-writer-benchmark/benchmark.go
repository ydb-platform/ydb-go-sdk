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

// Fixed master baseline measured on 2026-10-01 at aaf92e41 with Go 1.26.4 and
// ydbplatform/local-ydb:26.3.1.16. The fixed matrix used -benchtime=10s
// -count=3 -cpu=4 and one 1024-byte message per transaction. Each attempt used
// query.WithLazyTx(true), created the Topic writer before UPSERT materialized
// the transaction, and then called Write. The table contains medians; every
// fixed run had zero retries and zero final failures.
//
//	P    scenario                   tx/s    p95 ms      B/op  allocs/op  StreamWrite/tx
//	64   single                    198.5     21.29     78098       1234               1
//	64   many-key                  22.34     251.8   2221079      33058              64
//	64   many-bounded-key          20.81     274.1   2249062      33435              64
//	128  single                    153.1     38.37     78133       1237               1
//	128  many-key                  12.17     435.9   4378949      65027             128
//	128  many-bounded-key          8.913     699.5   4451088      65848             128
//	256  single                      129     49.81     78054       1237               1
//	256  many-key                  5.813     997.2   8726644     129409             256
//	256  many-bounded-key          4.772      1505   8820197     130633             256
//	512  single                    119.9     51.89     77967       1236               1
//	512  many-key                  1.587      3339  17371899     257991             512
//	512  many-bounded-key          1.026      4330  17755273     262297             512
//
// The auto-split benchmark was measured on 2026-10-01. It used -benchtime=1x
// -count=1 -cpu=4 three times, where one benchmark operation is a two-minute
// phase and every repetition used a fresh YDB container. Its medians were 20.21
// tx/s, 67.99ms p95, 1 -> 7 active partitions, 0.002043 retries/tx, and 5.967
// StreamWrite calls per committed transaction, with 7 final errors. Preserve
// this protocol and environment when comparing a candidate change.

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
