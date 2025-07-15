//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestTopicReadMessages(t *testing.T) {
	ctx := xtest.Context(t)

	db, reader := createFeedAndReader(ctx, t)

	sendCDCMessage(ctx, t, db)
	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, msg.CreatedAt)
	t.Logf("msg: %#v", msg)

	require.NoError(t, err)
	err = topicsugar.ReadMessageDataWithCallback(msg, func(data []byte) error {
		t.Log("Content:", string(data))
		return nil
	})
	require.NoError(t, err)

	sendCDCMessage(ctx, t, db)
	batch, err := reader.ReadMessageBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)
}

func TestRegression1802_StartPartitionWithOffsetHandler(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	scope.Require.NoError(scope.TopicWriter().Write(ctx,
		topicwriter.Message{},
		topicwriter.Message{},
		topicwriter.Message{},
	))

	msg, err := scope.TopicReaderNamed("first").ReadMessage(ctx)
	scope.Require.NoError(err)
	err = scope.TopicReaderNamed("first").Commit(ctx, msg)
	scope.Require.NoError(err)
	err = scope.TopicReaderNamed("first").Close(ctx)
	scope.Require.NoError(err)

	reader, err := scope.DriverWithGRPCLogging().Topic().StartReader(
		scope.TopicConsumerName(), topicoptions.ReadTopic(scope.TopicPath()),
		topicoptions.WithReaderGetPartitionStartOffset(func(ctx context.Context, req topicoptions.GetPartitionStartOffsetRequest) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
			res.StartFrom(2)
			return res, nil
		}),
	)
	scope.Require.NoError(err)

	msg, err = reader.ReadMessage(ctx)
	scope.Require.NoError(err)

	scope.Logf("Received message offset: %v", msg.Offset)

	err = reader.Commit(ctx, msg)
	scope.Require.NoError(err)

	scope.Require.EventuallyWithT(func(t *assert.CollectT) {
		c, err := scope.Driver().Topic().DescribeTopicConsumer(ctx, scope.TopicPath(), scope.TopicConsumerName(), topicoptions.IncludeConsumerStats())
		require.NoError(t, err)

		require.EqualValues(t, 3, c.Partitions[0].PartitionConsumerStats.CommittedOffset)
	}, 10*time.Second, 100*time.Millisecond)
}
