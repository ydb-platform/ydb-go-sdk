package transactionalwriterbenchmark

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
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
// the transaction, and then called Write. Every fixed run had zero final
// failures. The raw Go benchmark output was:
/*
BenchmarkTransactionalWriter/p64/single-4	2200	5036600 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p64/single-4	2462	5001744 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p64/single-4	2406	5123537 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p64/many-key-4	416	45144961 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-key-4	344	44764447 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-key-4	271	41235433 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-bounded-key-4	291	48043881 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-bounded-key-4	271	45016214 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p64/many-bounded-key-4	272	48681808 ns/op	0 errors	64.00 streams/tx
BenchmarkTransactionalWriter/p128/single-4	1834	6530249 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p128/single-4	1737	6608322 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p128/single-4	2014	6482218 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p128/many-key-4	138	78284050 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-key-4	177	82167643 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-key-4	153	83373821 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-bounded-key-4	87	115658159 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-bounded-key-4	165	83402487 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p128/many-bounded-key-4	127	112196606 ns/op	0 errors	128.0 streams/tx
BenchmarkTransactionalWriter/p256/single-4	1633	7742370 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p256/single-4	1647	8465182 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p256/single-4	1683	7752079 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p256/many-key-4	70	183954268 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-key-4	64	172030198 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-key-4	63	170444968 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-bounded-key-4	58	209538608 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-bounded-key-4	51	206661558 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p256/many-bounded-key-4	66	225842906 ns/op	0 errors	256.0 streams/tx
BenchmarkTransactionalWriter/p512/single-4	1624	7571217 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p512/single-4	1428	8338628 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p512/single-4	1560	8429209 ns/op	0 errors	1.000 streams/tx
BenchmarkTransactionalWriter/p512/many-key-4	24	501249553 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-key-4	21	630094687 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-key-4	13	968463503 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-bounded-key-4	12	1000694106 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-bounded-key-4	12	975123006 ns/op	0 errors	512.0 streams/tx
BenchmarkTransactionalWriter/p512/many-bounded-key-4	14	754791876 ns/op	0 errors	512.0 streams/tx
*/
//
// The auto-split benchmark used -benchtime=300x -count=1 -cpu=4 three times.
// Every operation is one transaction, and every repetition used a fresh YDB
// container. Its raw output was:
/*
BenchmarkTransactionalWriterAutoSplit-4	300	201452459 ns/op	2.000 errors	4.000 partitions	2.745 streams/tx
BenchmarkTransactionalWriterAutoSplit-4	300	202494712 ns/op	2.000 errors	4.000 partitions	2.738 streams/tx
BenchmarkTransactionalWriterAutoSplit-4	300	203012633 ns/op	1.000 errors	4.000 partitions	2.759 streams/tx
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
	execute            func(context.Context, uint64) error
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
) error {
	return r.executeTransaction(ctx, "")
}

func (r *transactionRunner) executeManyWriterTransaction(
	ctx context.Context,
	transactionNumber uint64,
) error {
	return r.executeTransaction(ctx, r.messageKey(transactionNumber))
}

func (r *transactionRunner) messageKey(transactionNumber uint64) string {
	return r.messageKeyPrefix + strconv.FormatUint(transactionNumber, 10)
}

func (r *transactionRunner) executeTransaction(
	parent context.Context,
	messageKey string,
) error {
	transactionContext, cancel := context.WithTimeout(parent, r.transactionTimeout)
	defer cancel()

	return r.db.Query().DoTx(
		transactionContext,
		func(ctx context.Context, tx query.TxActor) error {
			writer, err := r.db.Topic().StartTransactionalWriter(
				tx,
				r.topicPath,
				r.writerOptions...,
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
