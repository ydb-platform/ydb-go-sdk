//go:build integration
// +build integration

package integration

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
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

func TestReader(t *testing.T) {
	ctx := context.Background()

	ydbClient := connect(t)
	createCDCFeed(ctx, t, ydbClient)

	topicName := "topic_0"
	consumerName := "consumer_0"
	topic := ydbClient.Topic()
	err := topic.Create(ctx, topicName, topicoptions.CreateWithMinActivePartitions(1))
	require.NoError(t, err)

	w, err := topic.StartWriter(topicName, topicoptions.WithWriterWaitServerAck(true))
	require.NoError(t, err)
	for i := range 1000 {
		require.NoError(t, w.Write(ctx, topicwriter.Message{Data: strings.NewReader(strconv.Itoa(i))}))
	}
	require.NoError(t, w.Flush(ctx))

	require.NoError(t, topic.Alter(ctx, topicName, topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: consumerName})))

	var lastMessage *topicreader.Message
	t.Run("read1", func(t *testing.T) {
		readSelector := topicoptions.ReadSelector{
			Path:       topicName,
			Partitions: []int64{0},
		}

		r, err := topic.StartReader(consumerName, []topicoptions.ReadSelector{readSelector},
			topicoptions.WithReaderCommitMode(topicoptions.CommitModeAsync),
			topicoptions.WithReaderGetPartitionStartOffset(func(ctx context.Context, req topicoptions.GetPartitionStartOffsetRequest) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
				return res, nil
			}),
		)
		require.NoError(t, err)

		batch, err := r.ReadMessagesBatch(ctx)
		require.NoError(t, err)
		require.NoError(t, r.Commit(ctx, batch))
		lastMessage = Last(batch.Messages)

		require.EventuallyWithT(t, func(t *assert.CollectT) {
			c, err := topic.DescribeTopicConsumer(ctx, topicName, consumerName, topicoptions.IncludeConsumerStats())
			require.NoError(t, err)

			require.EqualValues(t, lastMessage.Offset+1, c.Partitions[0].PartitionConsumerStats.CommittedOffset)
		}, 10*time.Second, 100*time.Millisecond)

		require.NoError(t, r.Close(ctx))
	})

	for i := range 1000 {
		require.NoError(t, w.Write(ctx, topicwriter.Message{Data: strings.NewReader(strconv.Itoa(1000 + i))}))
	}
	require.NoError(t, w.Flush(ctx))

	c, err := topic.DescribeTopicConsumer(ctx, topicName, consumerName, topicoptions.IncludeConsumerStats())
	require.NoError(t, err)
	t.Logf("Last message ofset: %d, commited: %d", lastMessage.Offset, c.Partitions[0].PartitionConsumerStats.CommittedOffset)
	const offset = 5

	t.Run("read1.5", func(t *testing.T) {
		return // uncomment to fix the test

		readSelector := topicoptions.ReadSelector{
			Path:       topicName,
			Partitions: []int64{0},
		}

		r, err := topic.StartReader(consumerName, []topicoptions.ReadSelector{readSelector},
			topicoptions.WithReaderCommitMode(topicoptions.CommitModeAsync),
			topicoptions.WithReaderGetPartitionStartOffset(func(ctx context.Context, req topicoptions.GetPartitionStartOffsetRequest) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
				res.StartFrom(lastMessage.Offset + offset)
				return res, nil
			}),
		)
		require.NoError(t, err)

		c, err := topic.DescribeTopicConsumer(ctx, topicName, consumerName, topicoptions.IncludeConsumerStats())
		require.NoError(t, err)
		t.Logf("1.5 Commited offset before read: %d", c.Partitions[0].PartitionConsumerStats.CommittedOffset)

		_, err = r.ReadMessagesBatch(ctx)
		require.NoError(t, err)

		c, err = topic.DescribeTopicConsumer(ctx, topicName, consumerName, topicoptions.IncludeConsumerStats())
		require.NoError(t, err)
		t.Logf("1.5 Commited offset after read: %d", c.Partitions[0].PartitionConsumerStats.CommittedOffset)

		require.NoError(t, r.Close(ctx))
	})

	t.Run("read2", func(t *testing.T) {
		readSelector := topicoptions.ReadSelector{
			Path:       topicName,
			Partitions: []int64{0},
		}

		r, err := topic.StartReader(consumerName, []topicoptions.ReadSelector{readSelector},
			topicoptions.WithReaderCommitMode(topicoptions.CommitModeAsync),
			topicoptions.WithReaderGetPartitionStartOffset(func(ctx context.Context, req topicoptions.GetPartitionStartOffsetRequest) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
				res.StartFrom(lastMessage.Offset + offset)
				return res, nil
			}),
		)
		require.NoError(t, err)

		c, err := topic.DescribeTopicConsumer(ctx, topicName, consumerName, topicoptions.IncludeConsumerStats())
		require.NoError(t, err)
		t.Logf("Commited offset before read: %d", c.Partitions[0].PartitionConsumerStats.CommittedOffset)

		batch, err := r.ReadMessagesBatch(ctx)
		require.NoError(t, err)

		c, err = topic.DescribeTopicConsumer(ctx, topicName, consumerName, topicoptions.IncludeConsumerStats())
		require.NoError(t, err)
		t.Logf("Commited offset after read: %d", c.Partitions[0].PartitionConsumerStats.CommittedOffset)

		msg := batch.Messages[0]
		d, err := io.ReadAll(msg)
		require.NoError(t, err)
		d0, err := io.ReadAll(lastMessage)
		require.NoError(t, err)
		d0int, err := strconv.Atoi(string(d0))
		require.NoError(t, err)
		require.EqualValues(t, strconv.Itoa(d0int+offset), string(d))

		require.NoError(t, err)
		require.NoError(t, r.Commit(ctx, batch))

		require.NoError(t, r.Close(ctx))

		require.EventuallyWithT(t, func(t *assert.CollectT) {
			c, err := topic.DescribeTopicConsumer(ctx, topicName, consumerName, topicoptions.IncludeConsumerStats())
			require.NoError(t, err)

			require.EqualValues(t, Last(batch.Messages).Offset+1, c.Partitions[0].PartitionConsumerStats.CommittedOffset)
		}, 10*time.Second, 100*time.Millisecond)
	})
}

// Last returns the last element or a zero value if there are no elements.
func Last[T any](ss []T) T {
	if len(ss) == 0 {
		var zeroValue T

		return zeroValue
	}

	return ss[len(ss)-1]
}
