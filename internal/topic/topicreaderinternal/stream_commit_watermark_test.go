package topicreaderinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestAsyncCommitAfterTransactionCompletion(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	var queued int64
	e.partitionSession.SetupCommitMetrics(&trace.Topic{OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
		queued += int64(info.MessagesCount)
	}}, e.reader.cfg.ReaderInfo)
	message := topicreadercommon.NewPublicMessageBuilder().PartitionSession(e.partitionSession).Offset(20).Build()
	batch, err := topicreadercommon.NewBatch(e.partitionSession, []*topicreadercommon.PublicMessage{message})
	require.NoError(t, err)
	transaction := newMockTransactionWrapper("session", "tx")
	require.NoError(t, e.reader.commitWithTransaction(e.ctx, transaction, batch))
	transaction.onCompleted[0](nil)
	require.Equal(t, int64(21), e.partitionSession.CommittedOffset().ToInt64())

	require.NoError(t, e.reader.Commit(e.ctx, topicreadercommon.GetCommitRange(message)))
	require.Zero(t, queued)
}
