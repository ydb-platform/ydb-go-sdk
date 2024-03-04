package topicreaderexamples

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

func getEndOffset(b *topicreader.Batch) int64 {
	panic("example stub")
}

func externalSystemCommit(ctx context.Context, topic string, partition, offset int64) error {
	panic("example stub")
}

func externalSystemLock(ctx context.Context, topic string, partition int64) error {
	panic("example stub")
}

func externalSystemUnlock(ctx context.Context, topic string, partition int64) error {
	panic("example stub")
}

func processBatch(ctx context.Context, batch *topicreader.Batch) {
	// recommend derive ctx from batch.Context() for handle signal about stop message processing
	panic("example stub")
}

func processMessage(ctx context.Context, m *topicreader.Message) {
	// recommend derive ctx from m.Context() for handle signal about stop message processing
	panic("example stub")
}

func readLastOffsetFromDB(ctx context.Context, topic string, partition int64) (int64, error) {
	panic("example stub")
}
