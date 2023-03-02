//go:build !fast
// +build !fast

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicPartitionsBalanced(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)
	topicPath := db.Name() + "/topic-" + t.Name()

	err := db.Topic().Drop(ctx, topicPath)
	if err != nil {
		require.True(t, ydb.IsOperationErrorSchemeError(err))
	}

	consumer := "test-consumer-" + t.Name()
	err = db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithMinActivePartitions(2),
		topicoptions.CreateWithPartitionCountLimit(2),
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumer}),
	)
	require.NoError(t, err)

	connectedPartitions := int32(0)
	var handled int32

	var sessionsMutex sync.Mutex
	sessions := map[int64]bool{}

	tracer := trace.Topic{
		OnReaderPartitionReadStartResponse: func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) { //nolint:lll
			atomic.StoreInt32(&handled, 1)

			atomic.AddInt32(&connectedPartitions, 1)
			return nil
		},
		OnReaderPartitionReadStopResponse: func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) { //nolint:lll
			atomic.StoreInt32(&handled, 1)

			sessionsMutex.Lock()
			defer sessionsMutex.Unlock()
			if sessions[startInfo.PartitionSessionID] {
				return nil
			}
			sessions[startInfo.PartitionSessionID] = true

			atomic.AddInt32(&connectedPartitions, -1)
			return nil
		},
	}
	firstReader, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath),
		topicoptions.WithReaderTrace(tracer),
	)
	require.NoError(t, err)

	readCtx, firstReaderStopRead := context.WithCancel(ctx)
	firstReaderReadStopped := make(empty.Chan)
	go func() {
		defer close(firstReaderReadStopped)

		for {
			if readCtx.Err() != nil {
				return
			}
			_, err = firstReader.ReadMessage(readCtx)
			if readCtx.Err() == nil {
				require.NoError(t, err)
			}
		}
	}()

	xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
		return atomic.LoadInt32(&connectedPartitions) == 2
	})

	readerSecond, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath))
	require.NoError(t, err)

	xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
		return atomic.LoadInt32(&connectedPartitions) == 1
	})

	require.NoError(t, readerSecond.Close(ctx))

	xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
		return atomic.LoadInt32(&connectedPartitions) == 2
	})

	firstReaderStopRead()
	xtest.WaitChannelClosed(t, firstReaderReadStopped)
	require.NoError(t, firstReader.Close(ctx))
}
