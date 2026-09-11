package topicreaderinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

func TestBatcher_DrainPartitionSessionTransfersItemsAndPreservesOtherSessions(t *testing.T) {
	session := &topicreadercommon.PartitionSession{}
	otherSession := &topicreadercommon.PartitionSession{}
	targetBatch := mustNewBatch(session, []*topicreadercommon.PublicMessage{{Offset: 0}})
	otherBatch := mustNewBatch(otherSession, []*topicreadercommon.PublicMessage{{Offset: 0}})
	rawMessage := &rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: 1}

	b := newBatcher()
	require.NoError(t, b.PushBatches(targetBatch))
	require.NoError(t, b.PushRawMessage(session, rawMessage))
	require.NoError(t, b.PushBatches(otherBatch))
	b.FlushPartitionSession(session)
	b.FlushPartitionSession(otherSession)

	require.Equal(t,
		[]batcherMessageOrderItem{
			newBatcherItemBatch(targetBatch),
			newBatcherItemRawMessage(rawMessage),
		},
		b.DrainPartitionSession(session),
	)
	_, targetSessionStillQueued := b.messages[session]
	require.False(t, targetSessionStillQueued)
	require.Equal(t,
		batcherMessagesMap{otherSession: batcherMessageOrderItems{newBatcherItemBatch(otherBatch)}},
		b.messages,
	)
	require.Equal(t, []*topicreadercommon.PartitionSession{otherSession}, b.sessionsForFlush)
	require.Nil(t, b.DrainPartitionSession(session))

	bAll := newBatcher()
	require.NoError(t, bAll.PushBatches(targetBatch))
	require.NoError(t, bAll.PushRawMessage(session, rawMessage))
	require.NoError(t, bAll.PushBatches(otherBatch))
	require.ElementsMatch(t,
		[]batcherMessageOrderItem{
			newBatcherItemBatch(targetBatch),
			newBatcherItemRawMessage(rawMessage),
			newBatcherItemBatch(otherBatch),
		},
		bAll.Drain(),
	)
	require.Empty(t, bAll.messages)
	require.Empty(t, bAll.sessionsForFlush)
}
