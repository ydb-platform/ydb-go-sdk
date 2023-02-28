package topicreaderexamples

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// PartitionStopHandled is example of sdk handle server signal about stop partition
func PartitionStopHandled(ctx context.Context, reader *topicreader.Reader) {
	batch, _ := reader.ReadMessageBatch(ctx)
	if len(batch.Messages) == 0 {
		return
	}

	batchContext := batch.Context() // batch.Context() will cancel when partition revoke by server or connection broke
	processBatch(batchContext, batch)
}

// PartitionGracefulStopHandled is example of sdk handle server signal about graceful stop partition
func PartitionGracefulStopHandled(ctx context.Context, db ydb.Connection) {
	reader, _ := db.Topic().StartReader("consumer", nil,
		topicoptions.WithBatchReadMinCount(1000),
	)

	for {
		batch, _ := reader.ReadMessageBatch(ctx) // <- if partition soft stop batch can be less, then 1000
		processBatch(batch.Context(), batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}
