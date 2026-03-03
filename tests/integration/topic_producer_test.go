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

func readMessages(ctx context.Context, count int, scope *scopeT) error {
	reader, err := scope.Driver().Topic().StartReader(consumerName, topicoptions.ReadTopic(scope.TopicPath()))
	if err != nil {
		return err
	}

	expectedSeqNo := int64(1)
	for i := 0; i < count; i++ {
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

		if mess.SeqNo != expectedSeqNo {
			return fmt.Errorf("expected seq no %d, got %d", expectedSeqNo, mess.SeqNo)
		}

		expectedSeqNo++
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
	producer, err := topicClient.CreateProducer(
		scope.TopicPath(),
		topicoptions.WithBasicWriterOptions(
			topicoptions.WithWriterSetAutoSeqNo(false),
		),
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
	require.NoError(t, readMessages(ctx, 1000, scope))
}
