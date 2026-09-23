package main

import (
	"bytes"
	"context"
	"fmt"
	"sort"
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

const benchmarkTableQueryTemplate = `
DECLARE $run_id AS Utf8;
DECLARE $worker_id AS Uint64;
DECLARE $seq_no AS Uint64;

UPSERT INTO %s (run_id, worker_id, seq_no, updated_at)
VALUES ($run_id, $worker_id, $seq_no, CurrentUtcTimestamp());
`

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
			topicoptions.CreateWithMaxActivePartitions(cfg.AutoSplitMaxPartitions),
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
	sort.Slice(activePartitionIDs, func(i, j int) bool {
		return activePartitionIDs[i] < activePartitionIDs[j]
	})

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
		return fmt.Errorf("auto-split topic %q has min_active_partitions=%d, want 1; use a fresh topic", cfg.TopicPath, settings.MinActivePartitions)
	case settings.MaxActivePartitions != cfg.AutoSplitMaxPartitions:
		return fmt.Errorf(
			"auto-split topic %q has max_active_partitions=%d, want %d; use a fresh topic",
			cfg.TopicPath,
			settings.MaxActivePartitions,
			cfg.AutoSplitMaxPartitions,
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

func monitorTopicTopology(
	ctx context.Context,
	db *ydb.Driver,
	topicPath string,
	pollInterval time.Duration,
	recorder *topologyRecorder,
) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			description, err := db.Topic().Describe(ctx, topicPath)
			if err != nil {
				if ctx.Err() == nil {
					recorder.recordError(fmt.Errorf("describe topic %q while monitoring auto-split: %w", topicPath, err))
				}
				continue
			}
			topology, err := topicTopologyFromDescription(description)
			if err != nil {
				recorder.recordError(err)
				continue
			}
			recorder.record(time.Now(), topology)
		}
	}
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

	phaseContext, cancel := context.WithTimeout(parent, duration)
	defer cancel()

	var finalErrors atomic.Uint64
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
				cancel,
				db,
				cfg,
				workerID,
				activePartitionIDs,
				payload,
				&sequences[workerID],
				&finalErrors,
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

	aborted := finalErrors.Load() >= uint64(cfg.MaxErrors)
	stats := mergeWorkerStats(perWorker, endedAt.Sub(startedAt), aborted)
	switch {
	case aborted:
		return stats, fmt.Errorf("phase aborted after reaching --max-errors=%d", cfg.MaxErrors)
	case parent.Err() != nil:
		return stats, parent.Err()
	default:
		return stats, nil
	}
}

func runWorker(
	ctx context.Context,
	phaseDone <-chan struct{},
	cancelPhase context.CancelFunc,
	db *ydb.Driver,
	cfg config,
	workerID int,
	activePartitionIDs []int64,
	payload []byte,
	sequence *atomic.Uint64,
	finalErrors *atomic.Uint64,
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
			if finalErrors.Add(1) >= uint64(cfg.MaxErrors) {
				cancelPhase()
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
	doTxOptions := []query.DoTxOption{query.WithIdempotent()}
	if !cfg.QueryRetries {
		doTxOptions = append(doTxOptions, query.WithRetryBudget(noRetryBudget))
	}
	err := db.Query().DoTx(
		transactionContext,
		func(ctx context.Context, tx query.TxActor) error {
			attempts++
			currentTimings := attemptTimings{}

			if !cfg.SkipTableWrite {
				tableStartedAt := time.Now()
				err := tx.Exec(
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
	if cfg.Routing == routingModeKey {
		multiWriterOptions = append(
			multiWriterOptions,
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
		)
	} else if cfg.Routing == routingModeBoundedKey {
		multiWriterOptions = append(
			multiWriterOptions,
			topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser()),
		)
	} else {
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
