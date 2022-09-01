package topicreader_test

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

func ExampleReader_ReadMessage() {
	ctx := context.TODO()
	reader := readerConnect()

	for {
		msg, _ := reader.ReadMessage(ctx)
		processMessage(msg.Context(), msg)
		_ = reader.Commit(msg.Context(), msg)
	}
}

func ExampleReader_Commit() {
	ctx := context.TODO()
	reader := readerConnect()

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)

		// Commit may be fast (by default) or sync, depends on reader settings
		_ = reader.Commit(batch.Context(), batch)
	}
}

func ExampleReader_ReadMessageBatch() {
	ctx := context.TODO()
	reader := readerConnect()

	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}

func processBatch(ctx context.Context, batch *topicreader.Batch) {
	// recommend derive ctx from batch.Context() for handle signal about stop message processing
	panic("example stub")
}

func processMessage(ctx context.Context, m *topicreader.Message) {
	// recommend derive ctx from m.Context() for handle signal about stop message processing
	panic("example stub")
}

func readerConnect() *topicreader.Reader {
	panic("example stub")
}
