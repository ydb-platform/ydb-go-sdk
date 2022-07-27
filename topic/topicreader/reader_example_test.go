//go:build go1.16

package topicreader_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Example_simpleReadMessagesWithErrorHandle() {
	ctx := context.TODO()
	reader := readerConnect()
	for {
		msg, _ := reader.ReadMessage(ctx)
		processMessage(msg)
	}
}

func Example_effectiveUnmarshalMessageContentToJSONStruct() {
	ctx := context.TODO()
	reader := readerConnect()

	type S struct {
		MyField int `json:"my_field"`
	}

	var v S
	msg, _ := reader.ReadMessage(ctx)
	_ = topicsugar.JSONUnmarshal(msg, &v)
}

func Example_handlePartitionHardOff_NeedRare() {
	ctx := context.TODO()
	reader := readerConnect()

	batch, _ := reader.ReadMessageBatch(ctx)
	if len(batch.Messages) == 0 {
		return
	}

	batchContext := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke

	buf := &bytes.Buffer{}
	for _, msg := range batch.Messages {
		if batchContext.Err() != nil {
			// if batch context cancelled - it mean client need to stop process messages from batch
			// next messages will send to other reader
			return
		}
		_, _ = buf.ReadFrom(msg)
		writeBatchToDB(ctx, batch.Messages[0].WrittenAt, buf.Bytes())
	}
}

func Example_handlePartitionSoftOff_NeedRare() {
	ctx := context.TODO()
	db := dbConnect()
	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithBatchReadMinCount(1000),
	)

	for {
		batch, _ := reader.ReadMessageBatch(ctx) // <- if partition soft stop batch can be less, then 1000
		processBatch(batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}

func Example_simplePrintMessageContent() {
	ctx := context.TODO()
	reader := readerConnect()

	for {
		msg, _ := reader.ReadMessage(ctx)
		content, _ := io.ReadAll(msg)
		fmt.Println(string(content))
	}
}

func Example_readAndCommitEveryMessage() {
	ctx := context.TODO()
	reader := readerConnect()

	for {
		msg, _ := reader.ReadMessage(ctx)
		processMessage(msg)
		_ = reader.Commit(msg.Context(), msg)
	}
}

func Example_readMessagesWithAsyncBufferedCommit() {
	ctx := context.TODO()
	db := dbConnect()
	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithCommitMode(topicoptions.CommitModeAsync),
		topicoptions.WithCommitCountTrigger(1000),
	)
	defer func() {
		_ = reader.Close(ctx) // wait until flush buffered commits
	}()

	for {
		msg, _ := reader.ReadMessage(ctx)
		processMessage(msg)
		_ = reader.Commit(ctx, msg) // will fast - in async mode commit will append to internal buffer only
	}
}

func Example_readBatchesWithBatchCommit() {
	ctx := context.TODO()
	reader := readerConnect()

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}

func Example_readBatchWithMessageCommits() {
	ctx := context.TODO()
	reader := readerConnect()

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		for _, msg := range batch.Messages {
			processMessage(msg)
			_ = reader.Commit(msg.Context(), batch)
		}
	}
}

func Example_readMessagesWithCustomBatching() {
	ctx := context.TODO()
	db := dbConnect()

	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithBatchReadMinCount(1000),
	)

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}

func Example_readWithOwnReadProgressStorage() {
	ctx := context.TODO()
	db := dbConnect()

	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithGetPartitionStartOffset(
			func(
				ctx context.Context,
				req topicoptions.GetPartitionStartOffsetRequest,
			) (
				res topicoptions.GetPartitionStartOffsetResponse,
				err error,
			) {
				offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
				res.StartFrom(offset)

				// Reader will stop if return err != nil
				return res, err
			}),
	)

	for {
		batch, _ := reader.ReadMessageBatch(ctx)

		processBatch(batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
	}
}

func Example_readWithExplicitPartitionStartStopHandler() {
	ctx := context.TODO()
	db := dbConnect()

	readContext, stopReader := context.WithCancel(context.Background())
	defer stopReader()

	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithTracer(
			trace.Topic{
				OnPartitionReadStart: func(info trace.OnPartitionReadStartInfo) {
					err := externalSystemLock(info.PartitionContext, info.Topic, info.PartitionID)
					if err != nil {
						stopReader()
					}
				},
				OnPartitionReadStop: func(info trace.OnPartitionReadStopInfo) {
					if info.Graceful {
						err := externalSystemUnlock(ctx, info.Topic, info.PartitionID)
						if err != nil {
							stopReader()
						}
					}
				},
			},
		),
	)

	go func() {
		<-readContext.Done()
		_ = reader.Close(ctx)
	}()

	for {
		batch, _ := reader.ReadMessageBatch(readContext)

		processBatch(batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
	}
}

func Example_readWithExplicitPartitionStartStopHandlerAndOwnReadProgressStorage() {
	ctx := context.TODO()
	db := dbConnect()

	readContext, stopReader := context.WithCancel(context.Background())
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

	onPartitionStart := func(info trace.OnPartitionReadStartInfo) {
		err := externalSystemLock(info.PartitionContext, info.Topic, info.PartitionID)
		if err != nil {
			stopReader()
		}
	}

	onPartitionStop := func(info trace.OnPartitionReadStopInfo) {
		if info.Graceful {
			err := externalSystemUnlock(ctx, info.Topic, info.PartitionID)
			if err != nil {
				stopReader()
			}
		}
	}

	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),

		topicoptions.WithGetPartitionStartOffset(readStartPosition),
		topicoptions.WithTracer(
			trace.Topic{
				OnPartitionReadStart: onPartitionStart,
				OnPartitionReadStop:  onPartitionStop,
			},
		),
	)
	go func() {
		<-readContext.Done()
		_ = r.Close(ctx)
	}()

	for {
		batch, _ := r.ReadMessageBatch(readContext)

		processBatch(batch)
		_ = externalSystemCommit(batch.Context(), batch.Topic(), batch.PartitionID(), getEndOffset(batch))
	}
}

func Example_receiveCommitNotify() {
	ctx := context.TODO()
	db := dbConnect()

	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithTracer(trace.Topic{
			OnPartitionCommittedNotify: func(info trace.OnPartitionCommittedInfo) {
				// called when receive commit notify from server
				fmt.Println(info.Topic, info.PartitionID, info.CommittedOffset)
			},
		},
		),
	)

	for {
		msg, _ := reader.ReadMessage(ctx)
		processMessage(msg)
	}
}

func processBatch(batch *topicreader.Batch) {
	ctx := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke
	if len(batch.Messages) == 0 {
		return
	}

	buf := &bytes.Buffer{}
	for _, msg := range batch.Messages {
		buf.Reset()
		_, _ = buf.ReadFrom(msg)
		_, _ = io.Copy(buf, msg)
		writeMessagesToDB(ctx, buf.Bytes())
	}
}

func processMessage(m *topicreader.Message) {
	body, _ := io.ReadAll(m)
	writeToDB(
		m.Context(), // m.Context will skip if server revoke partition or connection to server broken
		m.SeqNo, body)
}

func writeToDB(ctx context.Context, id int64, body []byte) {
}

func writeBatchToDB(ctx context.Context, t time.Time, data []byte) {
}

func writeMessagesToDB(ctx context.Context, data []byte) {}

func externalSystemLock(ctx context.Context, topic string, partition int64) (err error) {
	panic("example stub")
}

func readLastOffsetFromDB(ctx context.Context, topic string, partition int64) (int64, error) {
	panic("example stub")
}

func externalSystemUnlock(ctx context.Context, topic string, partition int64) error {
	panic("example stub")
}

func externalSystemCommit(ctx context.Context, topic string, partition int64, offset int64) error {
	panic("example stub")
}

func getEndOffset(b *topicreader.Batch) int64 {
	panic("example stub")
}

func dbConnect() ydb.Connection {
	panic("example stub")
}

func readerConnect() *topicreader.Reader {
	panic("example stub")
}
