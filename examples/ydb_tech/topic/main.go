package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("All topic scenarios completed")
}

func run(ctx context.Context) (err error) {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	// [BEGIN topic_init]
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		return err
	}
	// [END topic_init]
	defer func() { err = errors.Join(err, db.Close(context.Background())) }()
	topicPath := fmt.Sprintf("ydb_tech_%d", time.Now().UnixNano())
	consumers := topicConsumers()

	// [BEGIN topic_create]
	err = db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),
		topicoptions.CreateWithMinActivePartitions(3),
		topicoptions.CreateWithMaxActivePartitions(3),
		topicoptions.CreateWithConsumer(consumers...),
	)
	if err != nil {
		return err
	}
	// [END topic_create]
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// [BEGIN topic_drop]
		dropErr := db.Topic().Drop(cleanupCtx, topicPath)
		// [END topic_drop]
		err = errors.Join(err, dropErr)
	}()
	// [BEGIN topic_alter]
	err = db.Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{
			Name: "another-consumer", SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
		}),
	)
	if err != nil {
		return err
	}
	// [END topic_alter]
	// [BEGIN topic_describe]
	description, err := db.Topic().Describe(ctx, topicPath)
	if err != nil {
		return err
	}
	fmt.Printf("Consumers: %d\n", len(description.Consumers))
	// [END topic_describe]
	if len(description.Consumers) != len(consumers)+1 {
		return fmt.Errorf("unexpected consumer count: %d", len(description.Consumers))
	}
	if err = write(ctx, db, topicPath); err != nil {
		return err
	}
	for _, consumer := range []string{"one", "batch", "commit_one", "commit_batch", "soft"} {
		if err = read(ctx, db, topicPath, consumer); err != nil {
			return err
		}
	}
	for _, scenario := range []func(context.Context, *ydb.Driver, string) error{
		readSelectors, readOwnOffsets, readWithoutConsumer, verifyHardStop, commitOutside, transactions, autoscaling,
	} {
		if err = scenario(ctx, db, topicPath); err != nil {
			return err
		}
	}

	return nil
}

var expected = map[string]int{
	"one": 1, "\x01\x02\x03": 1, "three": 1, "acknowledged": 1,
	"metadata": 1, "compressed": 1, "order-created": 1,
}

func write(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_start_writer]
	writer, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriterProducerID("ydb-tech-producer"),
		topicoptions.WithWriterPartitionID(0),
	)
	if err != nil {
		return err
	}
	// [END topic_start_writer]
	defer func() { err = errors.Join(err, writer.Close(context.Background())) }()
	// [BEGIN topic_write]
	err = writer.Write(ctx,
		topicwriter.Message{Data: strings.NewReader("one")},
		topicwriter.Message{Data: strings.NewReader("\x01\x02\x03")},
		topicwriter.Message{Data: strings.NewReader("three")},
	)
	if err != nil {
		return err
	}
	// [END topic_write]
	// [BEGIN topic_write_metadata]
	err = writer.Write(ctx, topicwriter.Message{
		Data: strings.NewReader("metadata"), Metadata: map[string][]byte{"meta-key": []byte("meta-value")},
	})
	if err != nil {
		return err
	}
	// [END topic_write_metadata]
	if err = writer.Flush(ctx); err != nil {
		return err
	}
	// [BEGIN topic_write_ack]
	ackWriter, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriterProducerID("ydb-tech-ack"),
		topicoptions.WithWriterWaitServerAck(true),
	)
	if err != nil {
		return err
	}
	err = ackWriter.Write(ctx, topicwriter.Message{Data: strings.NewReader("acknowledged")})
	if err != nil {
		return errors.Join(err, ackWriter.Close(context.Background()))
	}
	// [END topic_write_ack]
	if err = ackWriter.Close(ctx); err != nil {
		return err
	}
	if err = writeCompressed(ctx, db, topicPath); err != nil {
		return err
	}

	return writeManyPartitions(ctx, db, topicPath)
}

func read(ctx context.Context, db *ydb.Driver, topicPath, consumer string) (err error) {
	// [BEGIN topic_start_reader]
	reader, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	// [END topic_start_reader]
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	received := make(map[string]int)
	switch consumer {
	case "one":
		// [BEGIN topic_read_one]
		for total(received) < total(expected) {
			message, readErr := reader.ReadMessage(ctx)
			if readErr != nil {
				return readErr
			}
			if err = processMessage(message, received); err != nil {
				return err
			}
		}
		// [END topic_read_one]
	case "batch":
		// [BEGIN topic_read_batch]
		for total(received) < total(expected) {
			batch, readErr := reader.ReadMessagesBatch(ctx)
			if readErr != nil {
				return readErr
			}
			if err = processBatch(batch, received); err != nil {
				return err
			}
		}
		// [END topic_read_batch]
	case "commit_one":
		// [BEGIN topic_read_commit]
		for total(received) < total(expected) {
			message, readErr := reader.ReadMessage(ctx)
			if readErr != nil {
				return readErr
			}
			if err = processMessage(message, received); err != nil {
				return err
			}
			if err = reader.Commit(message.Context(), message); err != nil {
				return err
			}
		}
		// [END topic_read_commit]
	default:
		// [BEGIN topic_read_batch_commit]
		for total(received) < total(expected) {
			batch, readErr := reader.ReadMessagesBatch(ctx)
			if readErr != nil {
				return readErr
			}
			if err = processBatch(batch, received); err != nil {
				return err
			}
			if err = reader.Commit(batch.Context(), batch); err != nil {
				return err
			}
		}
		// [END topic_read_batch_commit]
	}

	return checkCounts(received)
}

func readSelectors(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	another := topicPath + "_another"
	if err = db.Topic().Create(ctx, another,
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "selectors"}),
	); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), another)) }()
	// [BEGIN topic_reader_selectors]
	reader, err := db.Topic().StartReader("selectors", topicoptions.ReadSelectors{
		{Path: topicPath},
		{Path: another, ReadFrom: time.Unix(0, 0)},
	})
	if err != nil {
		return err
	}
	// [END topic_reader_selectors]
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}

	return processMessage(message, make(map[string]int))
}

func readOwnOffsets(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	store := &offsetStore{offsets: make(map[int64]int64)}
	// [BEGIN topic_client_offset]
	reader, err := db.Topic().StartReader("offset", topicoptions.ReadTopic(topicPath),
		topicoptions.WithReaderCommitMode(topicoptions.CommitModeNone),
		topicoptions.WithReaderGetPartitionStartOffset(func(
			_ context.Context, request topicoptions.GetPartitionStartOffsetRequest,
		) (response topicoptions.GetPartitionStartOffsetResponse, err error) {
			response.StartFrom(store.Load(request.PartitionID))

			return response, nil
		}),
	)
	if err != nil {
		return err
	}
	// [END topic_client_offset]
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	batch, err := reader.ReadMessagesBatch(ctx)
	if err != nil {
		return err
	}
	if err = processBatch(batch, make(map[string]int)); err != nil {
		return err
	}
	if len(batch.Messages) == 0 {
		return fmt.Errorf("empty offset batch")
	}
	store.Save(batch.PartitionID(), batch.Messages[len(batch.Messages)-1].Offset+1)

	return nil
}

func readWithoutConsumer(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_no_consumer]
	reader, err := db.Topic().StartReader("", topicoptions.ReadSelectors{
		{Path: topicPath, Partitions: []int64{0, 1, 2}},
	}, topicoptions.WithReaderWithoutConsumer(false))
	if err != nil {
		return err
	}
	// [END topic_no_consumer]
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}

	return processMessage(message, make(map[string]int))
}

func hardStop(ctx context.Context, db *ydb.Driver, topicPath string) error {
	reader, err := db.Topic().StartReader("hard", topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	batch, err := reader.ReadMessagesBatch(ctx, topicreader.WithBatchMaxCount(1))
	if err != nil {
		return errors.Join(err, reader.Close(context.Background()))
	}
	if err = reader.Close(ctx); err != nil {
		return err
	}
	// [BEGIN topic_hard_stop]
	for _, message := range batch.Messages {
		if err = batch.Context().Err(); err != nil {
			return err
		}
		if err = processMessage(message, make(map[string]int)); err != nil {
			return err
		}
	}
	// [END topic_hard_stop]
	return fmt.Errorf("closing the reader did not expire the batch")
}

func verifyHardStop(ctx context.Context, db *ydb.Driver, topicPath string) error {
	err := hardStop(ctx, db, topicPath)
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("expected an expired batch context, got: %w", err)
	}

	return nil
}

func commitOutside(ctx context.Context, db *ydb.Driver, topicPath string) error {
	reader, err := db.Topic().StartReader("outside", topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return errors.Join(err, reader.Close(context.Background()))
	}
	// [BEGIN topic_commit_outside_session]
	err = db.Topic().CommitOffset(ctx, topicPath, message.PartitionID(), "outside", message.Offset+1,
		topicoptions.WithCommitOffsetReadSessionID(reader.ReadSessionID()),
	)
	// [END topic_commit_outside_session]
	err = errors.Join(err, reader.Close(context.Background()))
	if err != nil {
		return err
	}
	// [BEGIN topic_commit_outside]
	return db.Topic().CommitOffset(ctx, topicPath, message.PartitionID(), "outside", message.Offset+1)
	// [END topic_commit_outside]
}

func transactions(ctx context.Context, db *ydb.Driver, prefix string) (err error) {
	topicPath := prefix + "_tx"
	if err = db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "transaction"}),
	); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), topicPath)) }()
	// [BEGIN topic_write_tx]
	err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		writer, writeErr := db.Topic().StartTransactionalWriter(tx, topicPath)
		if writeErr != nil {
			return writeErr
		}

		return writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("transaction")})
	})
	// [END topic_write_tx]
	if err != nil {
		return err
	}
	reader, err := db.Topic().StartReader("transaction", topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	// [BEGIN topic_read_tx]
	err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		batch, readErr := reader.PopMessagesBatchTx(ctx, tx)
		if readErr != nil {
			return readErr
		}
		if len(batch.Messages) != 1 {
			return fmt.Errorf("expected one transactional message")
		}
		payload, readErr := io.ReadAll(batch.Messages[0])
		if readErr != nil {
			return readErr
		}
		if string(payload) != "transaction" {
			return fmt.Errorf("unexpected transactional payload: %q", payload)
		}

		return nil
	})
	// [END topic_read_tx]
	return err
}

func autoscaling(ctx context.Context, db *ydb.Driver, prefix string) (err error) {
	topicPath := prefix + "_auto"
	// [BEGIN topic_autoscale_create]
	err = db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithMinActivePartitions(1), topicoptions.CreateWithMaxActivePartitions(4),
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "auto"}),
		topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
			AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
			AutoPartitioningWriteSpeedStrategy: topictypes.AutoPartitioningWriteSpeedStrategy{
				StabilizationWindow: time.Minute, UpUtilizationPercent: 80,
			},
		}),
	)
	// [END topic_autoscale_create]
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), topicPath)) }()
	// [BEGIN topic_autoscale_alter]
	err = db.Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithAutoPartitioningStrategy(topictypes.AutoPartitioningStrategyScaleUp),
		topicoptions.AlterWithAutoPartitioningWriteSpeedStabilizationWindow(time.Minute),
		topicoptions.AlterWithAutoPartitioningWriteSpeedUpUtilizationPercent(80),
	)
	// [END topic_autoscale_alter]
	if err != nil {
		return err
	}
	writer, err := db.Topic().StartWriter(topicPath, topicoptions.WithWriterWaitServerAck(true))
	if err != nil {
		return err
	}
	err = writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("auto")})
	err = errors.Join(err, writer.Close(context.Background()))
	if err != nil {
		return err
	}
	// [BEGIN topic_autoscale_reader]
	for _, fullSupport := range []bool{true, false} {
		reader, startErr := db.Topic().StartReader("auto", topicoptions.ReadTopic(topicPath),
			topicoptions.WithReaderSupportSplitMergePartitions(fullSupport),
		)
		if startErr != nil {
			return startErr
		}
		message, readErr := reader.ReadMessage(ctx)
		if readErr != nil {
			return errors.Join(readErr, reader.Close(context.Background()))
		}
		payload, readErr := io.ReadAll(message)
		if readErr = errors.Join(readErr, reader.Close(context.Background())); readErr != nil {
			return readErr
		}
		if string(payload) != "auto" {
			return fmt.Errorf("unexpected autoscaling payload: %q", payload)
		}
	}
	// [END topic_autoscale_reader]
	return nil
}

func processMessage(message *topicreader.Message, received map[string]int) error {
	payload, err := io.ReadAll(message)
	if err != nil {
		return err
	}
	if _, ok := expected[string(payload)]; !ok {
		return fmt.Errorf("unexpected topic payload: %q", payload)
	}
	received[string(payload)]++
	if string(payload) == "metadata" {
		// [BEGIN topic_read_metadata]
		for key, value := range message.Metadata {
			fmt.Printf("%s: %s\n", key, value)
		}
		// [END topic_read_metadata]
		if string(message.Metadata["meta-key"]) != "meta-value" {
			return fmt.Errorf("unexpected message metadata")
		}
	}

	return nil
}

func processBatch(batch *topicreader.Batch, received map[string]int) error {
	for _, message := range batch.Messages {
		if err := processMessage(message, received); err != nil {
			return err
		}
	}

	return nil
}

func total(counts map[string]int) int {
	n := 0
	for _, count := range counts {
		n += count
	}

	return n
}

func checkCounts(received map[string]int) error {
	for payload, count := range expected {
		if received[payload] != count {
			return fmt.Errorf("unexpected count for %q: %d", payload, received[payload])
		}
	}

	return nil
}

type offsetStore struct {
	mu      sync.RWMutex
	offsets map[int64]int64
}

func (s *offsetStore) Load(partition int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.offsets[partition]
}

func (s *offsetStore) Save(partition, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[partition] = offset
}

func writeCompressed(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_codec]
	gzipWriter, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriterProducerID("ydb-tech-gzip"),
		topicoptions.WithWriterCodec(topictypes.CodecGzip),
		topicoptions.WithWriterWaitServerAck(true),
	)
	if err != nil {
		return err
	}
	// [END topic_codec]
	err = gzipWriter.Write(ctx, topicwriter.Message{Data: strings.NewReader("compressed")})
	err = errors.Join(err, gzipWriter.Close(context.Background()))
	if err != nil {
		return err
	}

	return nil
}

func writeManyPartitions(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_multiwriter]
	multiWriter, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithProducerIDPrefix("orders-producer"),
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
		),
	)
	if err != nil {
		return err
	}
	err = multiWriter.Write(ctx, topicwriter.Message{Key: "user-42", Data: strings.NewReader("order-created")})
	if err != nil {
		return errors.Join(err, multiWriter.Close(context.Background()))
	}
	// [END topic_multiwriter]
	return multiWriter.Close(ctx)
}

func topicConsumers() []topictypes.Consumer {
	return []topictypes.Consumer{
		{Name: "one"},
		{Name: "batch"},
		{Name: "commit_one"},
		{Name: "commit_batch"},
		{Name: "selectors"},
		{Name: "offset"},
		{Name: "hard"},
		{Name: "soft"},
		{Name: "outside"},
	}
}
