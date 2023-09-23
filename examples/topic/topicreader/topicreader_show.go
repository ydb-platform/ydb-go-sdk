package topicreaderexamples

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// PartitionStopHandled is example of sdk handle server signal about stop partition
func PartitionStopHandled(ctx context.Context, reader *topicreader.Reader) {
	batch, _ := reader.ReadMessagesBatch(ctx)
	if len(batch.Messages) == 0 {
		return
	}

	batchContext := batch.Context() // batch.Context() will cancel when partition revoke by server or connection broke
	processBatch(batchContext, batch)
}

// PartitionGracefulStopHandled is example of sdk handle server signal about graceful stop partition
func PartitionGracefulStopHandled(ctx context.Context, db *ydb.Driver) {
	reader, _ := db.Topic().StartReader("consumer", nil)

	for {
		batch, _ := reader.ReadMessagesBatch(ctx) // <- if partition soft stop batch can be less, then 1000
		processBatch(batch.Context(), batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}
