package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

var expected = map[string]int{"1": 2, "\x01\x02\x03": 2, "3": 2,
	"message-data": 1, "compressed": 1, "order-created": 1}
var activeProgress *progress
var offsetDB *ydb.Driver
var offsetTable string
var externalStop context.CancelFunc
var sinkError error

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
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close(context.Background())) }()
	topicPath := fmt.Sprintf("ydb_tech_%d", time.Now().UnixNano())
	if err = create(ctx, db, topicPath); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, drop(context.Background(), db, topicPath)) }()
	names := []string{"init", "one", "batch", "commit_one", "commit_batch", "selectors", "offset", "hard", "outside"}
	for _, name := range names {
		if err = db.Topic().Alter(ctx, topicPath,
			topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: name})); err != nil {
			return err
		}
	}
	if err = initialize(connectionString, topicPath); err != nil {
		return err
	}
	if err = manage(ctx, db, topicPath); err != nil {
		return err
	}
	if err = write(ctx, db, topicPath); err != nil {
		return err
	}
	if err = writeAcknowledged(ctx, db, topicPath); err != nil {
		return err
	}
	if err = writeCompressed(ctx, db, topicPath); err != nil {
		return err
	}
	if err = writeManyPartitions(ctx, db, topicPath); err != nil {
		return err
	}
	for _, consumer := range []string{"one", "batch", "commit_one", "commit_batch"} {
		if err = read(ctx, db, topicPath, consumer); err != nil {
			return err
		}
	}
	for _, scenario := range []func(context.Context, *ydb.Driver, string) error{
		metadata, readSelectors, readOwnOffsets, readWithoutConsumer, verifyHardStop,
		commitOutside, transactions, softStop, autoscaling,
	} {
		if err = scenario(ctx, db, topicPath); err != nil {
			return err
		}
	}
	return nil
}

func initialize(connectionString, topicPath string) (err error) {
	// [BEGIN topic_init]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close(ctx)) }()

	// db.Topic() — client for working with topics
	writer, err := db.Topic().StartWriter(topicPath)
	if err != nil {
		return err
	}
	reader, err := db.Topic().StartReader("init", topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	// [END topic_init]
	return errors.Join(writer.Close(ctx), reader.Close(ctx))
}

func create(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_create]
	err := db.Topic().Create(ctx, topicPath,
		// optional
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),

		// optional
		topicoptions.CreateWithMinActivePartitions(3),
	)
	// [END topic_create]
	return err
}

func drop(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_drop]
	err := db.Topic().Drop(ctx, topicPath)
	// [END topic_drop]
	return err
}

func manage(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_alter]
	err := db.Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{
			Name:            "new-consumer",
			SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}, // optional
		}),
	)
	// [END topic_alter]
	if err != nil {
		return err
	}
	// [BEGIN topic_describe]
	descResult, err := db.Topic().Describe(ctx, topicPath)
	if err != nil {
		return err
	}
	fmt.Printf("describe: %#v\n", descResult)
	// [END topic_describe]
	return nil
}

func write(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_start_writer]
	producerAndGroupID := "group-id"
	writer, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriterProducerID(producerAndGroupID),
	)
	if err != nil {
		return err
	}
	// [END topic_start_writer]
	defer func() { err = errors.Join(err, writer.Close(context.Background())) }()
	{
		// [BEGIN topic_write]
		err := writer.Write(ctx,
			topicwriter.Message{Data: strings.NewReader("1")},
			topicwriter.Message{Data: bytes.NewReader([]byte{1, 2, 3})},
			topicwriter.Message{Data: strings.NewReader("3")},
		)
		if err != nil {
			return err
		}
		// [END topic_write]
	}
	{
		// [BEGIN topic_write_metadata]
		err := writer.Write(ctx, topicwriter.Message{
			Data: strings.NewReader("message-data"),
			Metadata: map[string][]byte{
				"meta-key":    []byte("meta-value"),
				"another-key": []byte("value"),
			},
		})
		// [END topic_write_metadata]
		if err != nil {
			return err
		}
	}
	return writer.Flush(ctx)
}

func writeAcknowledged(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_write_ack]
	producerAndGroupID := "group-id"
	writer, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriterProducerID(producerAndGroupID),
		topicoptions.WithSyncWrite(true),
	)
	if err != nil {
		return err
	}
	err = writer.Write(ctx,
		topicwriter.Message{Data: strings.NewReader("1")},
		topicwriter.Message{Data: bytes.NewReader([]byte{1, 2, 3})},
		topicwriter.Message{Data: strings.NewReader("3")},
	)
	if err != nil {
		return err
	}
	// [END topic_write_ack]
	return writer.Close(ctx)
}

func writeCompressed(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_codec]
	producerAndGroupID := "group-id"
	writer, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriterProducerID(producerAndGroupID),
		topicoptions.WithCodec(topictypes.CodecGzip),
	)
	// [END topic_codec]
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, writer.Close(context.Background())) }()
	if err = writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("compressed")}); err != nil {
		return err
	}
	return writer.Flush(ctx)
}

func writeManyPartitions(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_multiwriter]
	writer, err := db.Topic().StartWriter(topicPath,
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithProducerIDPrefix("orders-producer"),
			topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser()),
		),
	)
	if err != nil {
		return err
	}
	defer func() { _ = writer.Close(context.Background()) }()

	err = writer.Write(ctx, topicwriter.Message{
		Key:  "user-42",
		Data: bytes.NewReader([]byte("order-created")),
	})
	if err != nil {
		return err
	}
	// [END topic_multiwriter]
	return writer.Flush(ctx)
}

func read(ctx context.Context, db *ydb.Driver, topicPath, consumer string) (err error) {
	// [BEGIN topic_start_reader]
	reader, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	// [END topic_start_reader]
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	readContext, cancel := context.WithCancel(ctx)
	defer cancel()
	activeProgress = &progress{expected: expected, received: make(map[string]int), cancel: cancel}
	defer func() { activeProgress = nil }()
	switch consumer {
	case "one":
		err = SimpleReadMessages(readContext, reader)
	case "batch":
		err = SimpleReadBatches(readContext, reader)
	case "commit_one":
		err = SimpleReadMessagesWithCommit(readContext, reader)
	case "commit_batch":
		err = SimpleReadMessageBatch(readContext, reader)
	}
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("reader stopped unexpectedly: %w", err)
	}
	return activeProgress.verify()
}

// [BEGIN topic_read_one]
func SimpleReadMessages(ctx context.Context, r *topicreader.Reader) error {
	for {
		mess, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		processMessage(mess)
	}
}

// [END topic_read_one]

// [BEGIN topic_read_batch]
func SimpleReadBatches(ctx context.Context, r *topicreader.Reader) error {
	for {
		batch, err := r.ReadMessagesBatch(ctx)
		if err != nil {
			return err
		}
		processBatch(batch)
	}
}

// [END topic_read_batch]

// [BEGIN topic_read_commit]
func SimpleReadMessagesWithCommit(ctx context.Context, r *topicreader.Reader) error {
	for {
		mess, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		processMessage(mess)
		if err := r.Commit(mess.Context(), mess); err != nil {
			return err
		}
	}
}

// [END topic_read_commit]

// [BEGIN topic_read_batch_commit]
func SimpleReadMessageBatch(ctx context.Context, r *topicreader.Reader) error {
	for {
		batch, err := r.ReadMessagesBatch(ctx)
		if err != nil {
			return err
		}
		processBatch(batch)
		if err := r.Commit(batch.Context(), batch); err != nil {
			return err
		}
	}
}

// [END topic_read_batch_commit]

func metadata(ctx context.Context, db *ydb.Driver, prefix string) (err error) {
	topicPath := prefix + "_metadata"
	if err = db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "metadata"})); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), topicPath)) }()
	writer, err := db.Topic().StartWriter(topicPath, topicoptions.WithSyncWrite(true))
	if err != nil {
		return err
	}
	err = writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("message-data"),
		Metadata: map[string][]byte{"meta-key": []byte("meta-value"), "another-key": []byte("value")}})
	err = errors.Join(err, writer.Close(ctx))
	if err != nil {
		return err
	}
	reader, err := db.Topic().StartReader("metadata", topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	// [BEGIN topic_read_metadata]
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}
	for k, v := range msg.Metadata {
		fmt.Printf("%s: %s\n", k, string(v))
	}
	// [END topic_read_metadata]
	return validateMessage(msg, make(map[string]int))
}

func readSelectors(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	another := topicPath + "_another"
	if err = db.Topic().Create(ctx, another,
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "selectors"})); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), another)) }()
	// [BEGIN topic_reader_selectors]
	reader, err := db.Topic().StartReader("selectors", topicoptions.ReadSelectors{
		{
			Path: topicPath,
		},
		{
			Path:     another,
			ReadFrom: time.Date(2022, 7, 1, 10, 15, 0, 0, time.UTC),
		},
	},
	)
	if err != nil {
		return err
	}
	// [END topic_reader_selectors]
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}
	return validateMessage(message, make(map[string]int))
}

func readWithoutConsumer(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_no_consumer]
	reader, err := db.Topic().StartReader(
		"",
		topicoptions.ReadSelectors{{
			Path:       topicPath,
			Partitions: []int64{0, 1, 2},
		}},
		topicoptions.WithReaderWithoutConsumer(false),
	)
	if err != nil {
		return err
	}
	// [END topic_no_consumer]
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}
	return validateMessage(message, make(map[string]int))
}

func readOwnOffsets(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	offsetDB = db
	offsetTable = topicPath + "_offsets"
	if err = db.Query().Exec(ctx, fmt.Sprintf("CREATE TABLE `%s` (topic Utf8, partition Int64, `offset` Int64, PRIMARY KEY(topic, partition))", offsetTable)); err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Query().Exec(context.Background(), fmt.Sprintf("DROP TABLE `%s`", offsetTable)))
	}()
	consumerName, topicName := "offset", topicPath
	{
		// [BEGIN topic_client_offset]
		reader, err := db.Topic().StartReader(
			consumerName,
			topicoptions.ReadTopic(topicName),
			topicoptions.WithReaderCommitMode(topicoptions.CommitModeNone),
		)
		// [END topic_client_offset]
		if err != nil {
			return err
		}
		if err = reader.Close(ctx); err != nil {
			return err
		}
	}
	readContext, cancel := context.WithCancel(ctx)
	defer cancel()
	externalStop = cancel
	activeProgress = &progress{expected: expected, received: make(map[string]int)}
	defer func() { activeProgress = nil; externalStop = nil }()
	err = ReadWithExplicitPartitionStartStopHandlerAndOwnReadProgressStorage(readContext, db, topicPath, consumerName)
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("external offset reader stopped unexpectedly: %w", err)
	}
	return activeProgress.verify()
}

// [BEGIN topic_client_offset_storage]
func ReadWithExplicitPartitionStartStopHandlerAndOwnReadProgressStorage(ctx context.Context, db *ydb.Driver, topicPath, consumerName string) error {
	readContext, stopReader := context.WithCancel(ctx)
	defer stopReader()

	readStartPosition := func(
		ctx context.Context,
		req topicoptions.GetPartitionStartOffsetRequest,
	) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
		offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
		res.StartFrom(offset)

		// Reader will stop if return err != nil
		return res, err
	}

	r, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath),
		topicoptions.WithGetPartitionStartOffset(readStartPosition),
	)
	if err != nil {
		return err
	}

	defer func() { _ = r.Close(context.Background()) }()

	for {
		batch, err := r.ReadMessagesBatch(readContext)
		if err != nil {
			return err
		}

		processBatch(batch)
		if err := externalSystemCommit(batch.Context(), batch.Topic(), batch.PartitionID(), batch.Messages[len(batch.Messages)-1].Offset+1); err != nil {
			return err
		}
	}
}

// [END topic_client_offset_storage]

func readLastOffsetFromDB(ctx context.Context, topic string, partition int64) (int64, error) {
	row, err := offsetDB.Query().QueryRow(ctx, fmt.Sprintf("DECLARE $topic AS Utf8; DECLARE $partition AS Int64; SELECT COALESCE(MAX(`offset`), CAST(0 AS Int64)) AS `offset` FROM `%s` WHERE topic = $topic AND partition = $partition", offsetTable),
		query.WithParameters(ydb.ParamsBuilder().Param("$topic").Text(topic).Param("$partition").Int64(partition).Build()))
	if err != nil {
		return 0, err
	}
	var offset int64
	err = row.ScanNamed(query.Named("offset", &offset))
	return offset, err
}

func externalSystemCommit(ctx context.Context, topic string, partition, offset int64) error {
	err := offsetDB.Query().Exec(ctx, fmt.Sprintf("DECLARE $topic AS Utf8; DECLARE $partition AS Int64; DECLARE $offset AS Int64; UPSERT INTO `%s` (topic, partition, `offset`) VALUES ($topic, $partition, $offset)", offsetTable),
		query.WithParameters(ydb.ParamsBuilder().Param("$topic").Text(topic).Param("$partition").Int64(partition).Param("$offset").Int64(offset).Build()))
	if err == nil && activeProgress.done() {
		externalStop()
	}
	return err
}

func verifyHardStop(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	sinkTable := topicPath + "_sink"
	if err = db.Query().Exec(ctx, fmt.Sprintf("CREATE TABLE `%s` (id Uint64, data String, PRIMARY KEY(id))", sinkTable)); err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Query().Exec(context.Background(), fmt.Sprintf("DROP TABLE `%s`", sinkTable)))
	}()
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
	sinkError = nil
	processStoppedBatch(db, sinkTable, batch)
	if !errors.Is(sinkError, context.Canceled) {
		return fmt.Errorf("expected an expired batch context, got: %w", sinkError)
	}
	row, err := db.Query().QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) AS count FROM `%s`", sinkTable))
	if err != nil {
		return err
	}
	var count uint64
	if err = row.ScanNamed(query.Named("count", &count)); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("expired batch was persisted")
	}
	return nil
}

func processStoppedBatch(db *ydb.Driver, sinkTable string, batch *topicreader.Batch) {
	writeMessagesToDB := func(ctx context.Context, payload []byte) {
		if len(payload) == 0 {
			sinkError = errors.New("empty hard-stop payload")
			return
		}
		sinkError = db.Query().Exec(ctx, fmt.Sprintf("DECLARE $data AS String; UPSERT INTO `%s` (id, data) VALUES (1u, $data)", sinkTable),
			query.WithParameters(ydb.ParamsBuilder().Param("$data").Bytes(payload).Build()))
	}
	// [BEGIN topic_hard_stop]
	ctx := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke
	if len(batch.Messages) == 0 {
		return
	}

	buf := &bytes.Buffer{}
	for _, mess := range batch.Messages {
		buf.Reset()
		_, _ = buf.ReadFrom(mess)
		_, _ = io.Copy(buf, mess)
		writeMessagesToDB(ctx, buf.Bytes())
	}
	// [END topic_hard_stop]
}

func commitOutside(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	consumer := "outside"
	reader, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}
	partitionID, offset := message.PartitionID(), message.Offset+1
	// [BEGIN topic_commit_outside_session]
	// Getting the read session identifier
	sessionID := reader.ReadSessionID()
	// or: sessionID := listener.ReadSessionID()

	err = db.Topic().CommitOffset(
		ctx,
		topicPath,
		partitionID,
		consumer,
		offset,
		topicoptions.WithCommitOffsetReadSessionID(sessionID),
	)
	// [END topic_commit_outside_session]
	if err != nil {
		return err
	}
	if err = reader.Close(ctx); err != nil {
		return err
	}
	{
		// [BEGIN topic_commit_outside]
		// Basic method — offset acknowledgment without an active read session
		err := db.Topic().CommitOffset(
			ctx,
			topicPath,
			partitionID,
			consumer,
			offset,
		)
		// [END topic_commit_outside]
		return err
	}
}

func transactions(ctx context.Context, db *ydb.Driver, prefix string) (err error) {
	topicName := prefix + "_tx"
	if err = db.Topic().Create(ctx, topicName,
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "transaction"})); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), topicName)) }()
	{
		// [BEGIN topic_write_tx]
		err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			writer, err := db.Topic().StartTransactionalWriter(tx, topicName)
			if err != nil {
				return err
			}

			return writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("asd")})
		})
		// [END topic_write_tx]
		if err != nil {
			return err
		}
	}
	reader, err := db.Topic().StartReader("transaction", topicoptions.ReadTopic(topicName))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	txContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return readTransactions(txContext, db, reader)
}

func readTransactions(ctx context.Context, db *ydb.Driver, reader *topicreader.Reader) (err error) {
	received := 0
	processTransactionBatch := func(_ context.Context, batch *topicreader.Batch) error {
		for _, message := range batch.Messages {
			payload, readErr := io.ReadAll(message)
			if readErr != nil {
				return readErr
			}
			if string(payload) != "asd" {
				return fmt.Errorf("unexpected transactional payload: %q", payload)
			}
			received++
		}
		return nil
	}
	handleError := func(failure error) { panic(failure) }
	defer func() {
		if failure := recover(); failure != nil {
			if readErr, ok := failure.(error); ok && errors.Is(readErr, context.DeadlineExceeded) && received == 1 {
				err = nil
			} else if ok {
				err = readErr
			} else {
				panic(failure)
			}
		}
	}()
	// [BEGIN topic_read_tx]
	for {
		err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			batch, err := reader.PopMessagesBatchTx(ctx, tx) // the batch will be committed upon the overall transaction commit
			if err != nil {
				return err
			}

			return processTransactionBatch(ctx, batch)
		})
		if err != nil {
			handleError(err)
		}
	}
	// [END topic_read_tx]
}

func softStop(ctx context.Context, db *ydb.Driver, prefix string) (err error) {
	topicPath := prefix + "_soft"
	if err = db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "my-consumer"})); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), topicPath)) }()
	writer, err := db.Topic().StartWriter(topicPath, topicoptions.WithSyncWrite(true))
	if err != nil {
		return err
	}
	messages := make([]topicwriter.Message, 1000)
	for index := range messages {
		messages[index].Data = strings.NewReader("1")
	}
	err = writer.Write(ctx, messages...)
	err = errors.Join(err, writer.Close(ctx))
	if err != nil {
		return err
	}
	readContext, cancel := context.WithCancel(ctx)
	defer cancel()
	activeProgress = &progress{expected: map[string]int{"1": 1000}, received: make(map[string]int), cancel: cancel}
	defer func() { activeProgress = nil }()
	err = readSoft(readContext, db, topicPath)
	if !errors.Is(err, context.Canceled) {
		return err
	}
	return activeProgress.verify()
}

func readSoft(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_soft_stop]
	r, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic(topicPath),
		topicoptions.WithBatchReadMinCount(1000),
	)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, r.Close(context.Background())) }()
	for {
		batch, err := r.ReadMessagesBatch(ctx) // if a partition soft stops, the batch can contain fewer than 1000 messages
		if err != nil {
			return err
		}
		processBatch(batch)
		if err := r.Commit(batch.Context(), batch); err != nil {
			return err
		}
	}
	// [END topic_soft_stop]
}

func autoscaling(ctx context.Context, db *ydb.Driver, prefix string) (err error) {
	topicPath := prefix + "_auto"
	basicPath := topicPath + "_basic"
	if err = createAutoBasic(ctx, db, basicPath); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), basicPath)) }()
	if err = createAuto(ctx, db, topicPath); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Topic().Drop(context.Background(), topicPath)) }()
	if err = alterAutoBasic(ctx, db, topicPath); err != nil {
		return err
	}
	if err = alterAuto(ctx, db, topicPath); err != nil {
		return err
	}
	if err = db.Topic().Alter(ctx, topicPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: "consumer"})); err != nil {
		return err
	}
	writer, err := db.Topic().StartWriter(topicPath, topicoptions.WithSyncWrite(true))
	if err != nil {
		return err
	}
	err = writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("auto")})
	err = errors.Join(err, writer.Close(ctx))
	if err != nil {
		return err
	}
	if err = readAutoFull(ctx, db, topicPath); err != nil {
		return err
	}
	return readAutoCompat(ctx, db, topicPath)
}

func createAutoBasic(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_autoscale_create_basic]
	err := db.Topic().Create(ctx,
		topicPath,
		topicoptions.CreateWithAutoPartitioningSettings(
			topictypes.AutoPartitioningSettings{
				AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
			},
		),
	)
	// [END topic_autoscale_create_basic]
	return err
}

func createAuto(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_autoscale_create]
	err := db.Topic().Create(ctx,
		topicPath,
		topicoptions.CreateWithAutoPartitioningSettings(
			topictypes.AutoPartitioningSettings{
				AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
				AutoPartitioningWriteSpeedStrategy: topictypes.AutoPartitioningWriteSpeedStrategy{
					StabilizationWindow:  time.Minute,
					UpUtilizationPercent: 80,
				},
			},
		),
	)
	// [END topic_autoscale_create]
	return err
}

func alterAutoBasic(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_autoscale_alter_basic]
	err := db.Topic().Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithAutoPartitioningStrategy(
			topictypes.AutoPartitioningStrategyScaleUp,
		),
	)
	// [END topic_autoscale_alter_basic]
	return err
}

func alterAuto(ctx context.Context, db *ydb.Driver, topicPath string) error {
	// [BEGIN topic_autoscale_alter]
	err := db.Topic().Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithAutoPartitioningStrategy(
			topictypes.AutoPartitioningStrategyScaleUp,
		),
		topicoptions.AlterWithAutoPartitioningWriteSpeedStabilizationWindow(time.Minute),
		topicoptions.AlterWithAutoPartitioningWriteSpeedUpUtilizationPercent(80),
	)
	// [END topic_autoscale_alter]
	return err
}

func readAutoFull(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_autoscale_reader_full]
	reader, err := db.Topic().StartReader(
		"consumer",
		topicoptions.ReadTopic(topicPath),
		topicoptions.WithReaderSupportSplitMergePartitions(true),
	)
	// [END topic_autoscale_reader_full]
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}
	payload, err := io.ReadAll(message)
	if err != nil {
		return err
	}
	if string(payload) != "auto" {
		return fmt.Errorf("unexpected autoscaling payload: %q", payload)
	}
	return nil
}

func readAutoCompat(ctx context.Context, db *ydb.Driver, topicPath string) (err error) {
	// [BEGIN topic_autoscale_reader_compat]
	reader, err := db.Topic().StartReader(
		"consumer",
		topicoptions.ReadTopic(topicPath),
		topicoptions.WithReaderSupportSplitMergePartitions(false),
	)
	// [END topic_autoscale_reader_compat]
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close(context.Background())) }()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return err
	}
	payload, err := io.ReadAll(message)
	if err != nil {
		return err
	}
	if string(payload) != "auto" {
		return fmt.Errorf("unexpected autoscaling payload: %q", payload)
	}
	return nil
}

type progress struct {
	expected map[string]int
	received map[string]int
	cancel   context.CancelFunc
	failure  error
}

func processMessage(message *topicreader.Message) {
	if err := validateMessage(message, activeProgress.received); err != nil {
		activeProgress.failure = err
	}
	if activeProgress.cancel != nil && (activeProgress.failure != nil || activeProgress.done()) {
		activeProgress.cancel()
	}
}

func processBatch(batch *topicreader.Batch) {
	for _, message := range batch.Messages {
		processMessage(message)
	}
}

func validateMessage(message *topicreader.Message, received map[string]int) error {
	payload, err := io.ReadAll(message)
	if err != nil {
		return err
	}
	if _, ok := expected[string(payload)]; !ok {
		return fmt.Errorf("unexpected topic payload: %q", payload)
	}
	received[string(payload)]++
	if string(payload) == "message-data" {
		if string(message.Metadata["meta-key"]) != "meta-value" || string(message.Metadata["another-key"]) != "value" {
			return errors.New("unexpected message metadata")
		}
	}
	return nil
}

func (p *progress) done() bool {
	for payload, count := range p.expected {
		if p.received[payload] < count {
			return false
		}
	}
	return true
}

func (p *progress) verify() error {
	if p.failure != nil {
		return p.failure
	}
	for payload, count := range p.expected {
		if p.received[payload] != count {
			return fmt.Errorf("unexpected count for %q: %d", payload, p.received[payload])
		}
	}
	return nil
}
