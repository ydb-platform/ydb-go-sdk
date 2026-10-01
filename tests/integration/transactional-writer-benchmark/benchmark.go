package transactionalwriterbenchmark

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// Fixed master baseline measured on 2026-09-30 at aaf92e41 with Go 1.26.4 and
// ydbplatform/local-ydb:26.3.1.16. The linux/amd64 test binary ran inside the
// YDB container because Colima host port forwarding was unavailable. The fixed
// matrix used -benchtime=10s -count=3 -cpu=4 and one 1024-byte message per
// transaction. Each attempt used query.WithLazyTx(true), created the Topic
// writer before UPSERT materialized the transaction, and then called Write.
// The table contains medians; every fixed run had zero retries and zero final
// failures.
//
//	P    scenario               tx/s   p95 ms      B/op  allocs/op  StreamWrite/tx
//	64   single                174.3     31.93     79746       1263               1
//	64   many-key               42.44   113.8    2231570      33285              64
//	64   many-bounded-key       21.64   252.3    2247785      33606              64
//	64   many-partition-id      22.77   259.9    2230435      33249              64
//	128  single                152.9     38.15     79643       1263               1
//	128  many-key               13.12   400.1    4387854      65352             128
//	128  many-bounded-key       10.26   560.0    4438662      66057             128
//	128  many-partition-id      11.56   584.2    4395358      65389             128
//	256  single                151.2     37.96     79533       1263               1
//	256  many-key                5.885  866.1    8725378     129891             256
//	256  many-bounded-key        5.244 1014      8818335     131231             256
//	256  many-partition-id       5.829  879.0    8729962     129842             256
//	512  single                140.2     42.49     79463       1263               1
//	512  many-key                2.835 1807     17347325     258656             512
//	512  many-bounded-key        1.941 3336     17629466     262029             512
//	512  many-partition-id       3.517 1464     17465050     259583             512
//
// The auto-split benchmark was measured on 2026-10-01. It used -benchtime=1x
// -count=1 -cpu=4 three times, where one benchmark operation is a two-minute
// phase and every repetition used a fresh YDB container. Its medians were 64.63
// tx/s, 158.8ms p95, 1 -> 10 active partitions, 0.001676 retries/tx, and 10.58
// StreamWrite calls per committed transaction. The median final error count was
// zero; one run recorded two final transaction errors. Preserve this protocol
// and environment when comparing a candidate change.

const benchmarkTableQueryTemplate = `
DECLARE $run_id AS Utf8;
DECLARE $worker_id AS Uint64;
DECLARE $seq_no AS Uint64;

UPSERT INTO %s (run_id, worker_id, seq_no, updated_at)
VALUES ($run_id, $worker_id, $seq_no, CurrentUtcTimestamp());
`

const ydbMaxTopicPartitions int64 = 35_000

var noRetryBudget = budget.Percent(0)

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
	WriterWrite time.Duration
}

type messageSpec struct {
	SeqNo       int64
	Key         string
	PartitionID int64
}

func prepareSchema(ctx context.Context, db *ydb.Driver, cfg config) error {
	if !cfg.SkipTableWrite {
		statement := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    run_id Utf8 NOT NULL,
    worker_id Uint64 NOT NULL,
    seq_no Uint64,
    updated_at Timestamp,
    PRIMARY KEY (run_id, worker_id)
);
`, quoteYQLPath(cfg.TablePath))
		if err := db.Query().Exec(ctx, statement, query.WithIdempotent()); err != nil {
			return fmt.Errorf("prepare table %q: %w", cfg.TablePath, err)
		}
	}

	description, err := db.Topic().Describe(ctx, cfg.TopicPath)
	if err == nil {
		if cfg.AutoSplit {
			if err := validateAutoSplitTopic(description, cfg); err != nil {
				return err
			}
		}

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
	activePartitionIDs := make([]int64, 0, len(description.Partitions))
	for _, partition := range description.Partitions {
		if partition.Active {
			activePartitionIDs = append(activePartitionIDs, partition.PartitionID)
		}
	}
	if len(activePartitionIDs) == 0 {
		return topicTopology{}, fmt.Errorf("topic %q has no active partitions", description.Path)
	}
	slices.Sort(activePartitionIDs)

	return topicTopology{
		ActivePartitionIDs: activePartitionIDs,
		TotalPartitions:    len(description.Partitions),
	}, nil
}

func validateAutoSplitTopic(description topictypes.TopicDescription, cfg config) error {
	settings := description.PartitionSettings
	writeSpeed := settings.AutoPartitioningSettings.AutoPartitioningWriteSpeedStrategy
	switch {
	case settings.MinActivePartitions != 1:
		return fmt.Errorf(
			"auto-split topic %q has min_active_partitions=%d, want 1; use a fresh topic",
			cfg.TopicPath,
			settings.MinActivePartitions,
		)
	case settings.MaxActivePartitions != ydbMaxTopicPartitions:
		return fmt.Errorf(
			"auto-split topic %q has max_active_partitions=%d, want server ceiling %d; use a fresh topic",
			cfg.TopicPath,
			settings.MaxActivePartitions,
			ydbMaxTopicPartitions,
		)
	case settings.AutoPartitioningSettings.AutoPartitioningStrategy != topictypes.AutoPartitioningStrategyScaleUp:
		return fmt.Errorf("auto-split topic %q does not use SCALE_UP; use a fresh topic", cfg.TopicPath)
	case writeSpeed.UpUtilizationPercent != int32(cfg.AutoSplitUpUtilization):
		return fmt.Errorf(
			"auto-split topic %q has up_utilization_percent=%d, want %d; use a fresh topic",
			cfg.TopicPath,
			writeSpeed.UpUtilizationPercent,
			cfg.AutoSplitUpUtilization,
		)
	case writeSpeed.StabilizationWindow != cfg.AutoSplitStabilization:
		return fmt.Errorf(
			"auto-split topic %q has stabilization_window=%s, want %s; use a fresh topic",
			cfg.TopicPath,
			writeSpeed.StabilizationWindow,
			cfg.AutoSplitStabilization,
		)
	case description.PartitionWriteSpeedBytesPerSecond != cfg.AutoSplitWriteSpeed:
		return fmt.Errorf(
			"auto-split topic %q has partition_write_speed=%d, want %d; use a fresh topic",
			cfg.TopicPath,
			description.PartitionWriteSpeedBytesPerSecond,
			cfg.AutoSplitWriteSpeed,
		)
	case description.PartitionWriteBurstBytes != cfg.AutoSplitBurstBytes:
		return fmt.Errorf(
			"auto-split topic %q has partition_write_burst=%d, want %d; use a fresh topic",
			cfg.TopicPath,
			description.PartitionWriteBurstBytes,
			cfg.AutoSplitBurstBytes,
		)
	}

	return nil
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
	db *ydb.Driver,
	cfg config,
	activePartitionIDs []int64,
	payload []byte,
	duration time.Duration,
	sequences []atomic.Uint64,
) (phaseStats, error) {
	if duration == 0 {
		return phaseStats{}, nil
	}

	phaseContext, cancelPhase := context.WithTimeout(parent, duration)
	defer cancelPhase()

	results := make(chan workerStats, cfg.Concurrency)
	startedAt := time.Now()

	var workers sync.WaitGroup
	workers.Add(cfg.Concurrency)
	for workerID := range cfg.Concurrency {
		go func() {
			defer workers.Done()
			results <- runWorker(
				parent,
				phaseContext.Done(),
				db,
				cfg,
				workerID,
				activePartitionIDs,
				payload,
				&sequences[workerID],
			)
		}()
	}

	<-phaseContext.Done()
	workers.Wait()
	endedAt := time.Now()
	close(results)

	perWorker := make([]workerStats, 0, cfg.Concurrency)
	for result := range results {
		perWorker = append(perWorker, result)
	}

	stats := mergeWorkerStats(perWorker, endedAt.Sub(startedAt))
	if parent.Err() != nil {
		return stats, parent.Err()
	}

	return stats, nil
}

//nolint:funlen // Keeping collection in the worker makes the measured path easy to audit.
func runWorker(
	ctx context.Context,
	phaseDone <-chan struct{},
	db *ydb.Driver,
	cfg config,
	workerID int,
	activePartitionIDs []int64,
	payload []byte,
	sequence *atomic.Uint64,
) workerStats {
	var stats workerStats

	for {
		select {
		case <-phaseDone:
			return stats
		default:
		}

		logicalSequence := sequence.Add(1)
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
			if ctx.Err() != nil {
				stats.Cancelled++

				continue
			}

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
}

//nolint:funlen // The complete measured transaction is intentionally kept together.
func executeTransaction(
	parent context.Context,
	db *ydb.Driver,
	cfg config,
	workerID int,
	sequence uint64,
	activePartitionIDs []int64,
	payload []byte,
) (time.Duration, attemptTimings, int, error) {
	transactionContext, cancel := context.WithTimeout(parent, cfg.TransactionTimeout)
	defer cancel()

	messageSpecs := makeMessageSpecs(cfg, workerID, sequence, activePartitionIDs)
	var (
		attempts    int
		lastTimings attemptTimings
	)
	startedAt := time.Now()
	doTxOptions := []query.DoTxOption{
		query.WithIdempotent(),
		query.WithLazyTx(true),
	}
	if !cfg.QueryRetries {
		doTxOptions = append(doTxOptions, query.WithRetryBudget(noRetryBudget))
	}
	err := db.Query().DoTx(
		transactionContext,
		func(ctx context.Context, tx query.TxActor) error {
			attempts++
			currentTimings := attemptTimings{}

			writerStartedAt := time.Now()
			writer, err := db.Topic().StartTransactionalWriter(
				tx,
				cfg.TopicPath,
				writerOptions(cfg, workerID)...,
			)
			currentTimings.WriterStart = time.Since(writerStartedAt)
			if err != nil {
				lastTimings = currentTimings

				return fmt.Errorf("start transactional writer: %w", err)
			}

			if !cfg.SkipTableWrite {
				tableStartedAt := time.Now()
				err = tx.Exec(
					ctx,
					fmt.Sprintf(benchmarkTableQueryTemplate, quoteYQLPath(cfg.TablePath)),
					query.WithParameters(
						ydb.ParamsBuilder().
							Param("$run_id").Text(cfg.RunID).
							Param("$worker_id").Uint64(uint64(workerID)).
							Param("$seq_no").Uint64(sequence).
							Build(),
					),
				)
				currentTimings.Table = time.Since(tableStartedAt)
				if err != nil {
					lastTimings = currentTimings

					return fmt.Errorf("execute benchmark table upsert: %w", err)
				}
			}

			writeStartedAt := time.Now()
			err = writer.Write(ctx, makeMessages(messageSpecs, payload)...)
			currentTimings.WriterWrite = time.Since(writeStartedAt)
			lastTimings = currentTimings
			if err != nil {
				return fmt.Errorf("write transactional topic messages: %w", err)
			}

			return nil
		},
		doTxOptions...,
	)

	return time.Since(startedAt), lastTimings, attempts, err
}

func writerOptions(cfg config, workerID int) []topicoptions.WriterOption {
	options := []topicoptions.WriterOption{
		topicoptions.WithWriterSetAutoSeqNo(cfg.AutoSeqNo),
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
	case routingModePartitionID:
		multiWriterOptions = append(multiWriterOptions, topicoptions.WithWriterPartitionByPartitionID())
	}
	if slotProducerID != "" {
		multiWriterOptions = append(multiWriterOptions, topicoptions.WithProducerIDPrefix(slotProducerID))
	}

	return append(options, topicoptions.WithWriteToManyPartitions(multiWriterOptions...))
}

func makeMessageSpecs(
	cfg config,
	workerID int,
	sequence uint64,
	activePartitionIDs []int64,
) []messageSpec {
	specs := make([]messageSpec, cfg.MessagesPerTx)
	for index := range specs {
		messageSequence := (sequence-1)*uint64(cfg.MessagesPerTx) + uint64(index) + 1
		if !cfg.AutoSeqNo {
			specs[index].SeqNo = int64(messageSequence)
		}
		if cfg.Mode != writerModeMany {
			continue
		}
		if cfg.Routing == routingModeKey || cfg.Routing == routingModeBoundedKey {
			specs[index].Key = fmt.Sprintf("worker-%d-message-%d", workerID, messageSequence)

			continue
		}

		partitionIndex := (messageSequence + uint64(workerID) - 1) % uint64(len(activePartitionIDs))
		specs[index].PartitionID = activePartitionIDs[partitionIndex]
	}

	return specs
}

func makeMessages(specs []messageSpec, payload []byte) []topicwriter.Message {
	messages := make([]topicwriter.Message, len(specs))
	for i := range specs {
		messages[i] = topicwriter.Message{
			SeqNo:       specs[i].SeqNo,
			Key:         specs[i].Key,
			PartitionID: specs[i].PartitionID,
			Data:        bytes.NewReader(payload),
		}
	}

	return messages
}

func quoteYQLPath(path string) string {
	return "`" + path + "`"
}
