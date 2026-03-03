//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicproducer"
)

func readMessages(ctx context.Context, count int, topicPath string, scope *scopeT) error {
	reader, err := scope.Driver().Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
	if err != nil {
		return err
	}

	partitionsSeqNoMap := make(map[int64][]int64)

	for i := range count {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		mess, err := reader.ReadMessage(readCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if i != count-1 {
					return errors.New("not all messages read")
				}

				return nil
			}

			return err
		}

		partitionID := mess.PartitionID()
		seqNos, ok := partitionsSeqNoMap[partitionID]
		if !ok {
			partitionsSeqNoMap[partitionID] = []int64{mess.SeqNo}

			continue
		}

		if len(seqNos) > 0 && seqNos[len(seqNos)-1] > mess.SeqNo {
			return fmt.Errorf("seq no is not in order for partition %d", partitionID)
		}

		seqNos = append(seqNos, mess.SeqNo)
		partitionsSeqNoMap[partitionID] = seqNos
	}

	return nil
}

// TestTopicProducer_WaitInitAndClose verifies that internal topic producer
// can be initialized and closed against a real YDB topic.
func TestTopicProducer_WaitInitAndClose(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	producer, err := topicClient.CreateProducer(scope.TopicPath())
	require.NoError(t, err)

	err = producer.WaitInit(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.NoError(t, err)
}

func TestTopicProducer_WriteAndFlush(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()

	// Create topic with 10 partitions for this test.
	topicPath := createTopic(ctx, t, scope.Driver())
	err := topicClient.Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithMinActivePartitions(10),
		topicoptions.AlterWithMaxActivePartitions(10),
	)
	require.NoError(t, err)

	producer, err := topicClient.CreateProducer(
		topicPath,
		topicoptions.WithBasicWriterOptions(
			topicoptions.WithWriterSetAutoSeqNo(false),
		),
		topicoptions.WithPartitionChooserStrategy(topicproducer.PartitionChooserStrategyHash),
	)
	require.NoError(t, err)

	err = producer.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]topicproducer.Message, 0, 1000)
	for i := range 1000 {
		messages = append(messages, topicproducer.Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 1),
			},
			Key: fmt.Sprintf("partition-key-%d", i),
		})
	}

	require.NoError(t, producer.Write(ctx, messages...))
	require.NoError(t, producer.Close(ctx))

	stats := producer.GetWriteStats()
	require.Equal(t, int64(1000), stats.MessagesWritten)
	require.Equal(t, int64(1000), stats.LastWrittenSeqNo)
	require.NoError(t, readMessages(ctx, 1000, topicPath, scope))
}
