//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
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

	var connectedPartitions atomic.Int64
	var handled atomic.Int64

	var sessionsMutex sync.Mutex
	sessions := map[int64]bool{}

	tracer := trace.Topic{
		OnReaderPartitionReadStartResponse: func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) { //nolint:lll
			handled.Store(1)

			connectedPartitions.Add(1)
			return nil
		},
		OnReaderPartitionReadStopResponse: func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) { //nolint:lll
			handled.Store(1)

			sessionsMutex.Lock()
			defer sessionsMutex.Unlock()
			if sessions[startInfo.PartitionSessionID] {
				return nil
			}
			sessions[startInfo.PartitionSessionID] = true

			connectedPartitions.Add(-1)
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
		return connectedPartitions.Load() == 2
	})

	readerSecond, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath))
	require.NoError(t, err)

	xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
		return connectedPartitions.Load() == 1
	})

	require.NoError(t, readerSecond.Close(ctx))

	xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
		return connectedPartitions.Load() == 2
	})

	firstReaderStopRead()
	xtest.WaitChannelClosed(t, firstReaderReadStopped)
	require.NoError(t, firstReader.Close(ctx))
}

func TestTopicSplitPartitions(t *testing.T) {
	if os.Getenv("YDB_VERSION") != "nightly" && version.Lt(os.Getenv("YDB_VERSION"), "25.0") {
		t.Skip("require support autosplit for topics")
	}
	scope := newScope(t)

	params := `
max_active_partitions=2, 
partition_write_speed_bytes_per_second=1,
auto_partitioning_strategy='scale_up',
auto_partitioning_up_utilization_percent=1,
auto_partitioning_stabilization_window=Interval('PT1S')
`
	err := scope.Driver().Topic().Drop(scope.Ctx, scope.TopicPath())
	scope.Require.NoError(err)

	err = scope.Driver().Query().Exec(scope.Ctx, fmt.Sprintf("CREATE TOPIC `%v` (consumer `%v`) WITH (%v)",
		scope.TopicPath(), scope.TopicConsumerName(), params))
	scope.Require.NoError(err)

	t.Log("Start writers")
	var messagesCounter atomic.Int64
	var writersStop atomic.Bool
	var writers sync.WaitGroup
	for i := 0; i < 10; i++ {
		writers.Add(1)
		go func(index int) {
			defer writers.Done()

			writer, err := scope.Driver().Topic().StartWriter(scope.TopicPath(), topicoptions.WithWriterWaitServerAck(false))
			scope.Require.NoError(err)

			for !writersStop.Load() {
				err = writer.Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("-")})
				scope.Require.NoError(err)
				messagesCounter.Add(1)
			}
			err = writer.Close(scope.Ctx)
			scope.Require.NoError(err)
		}(i)
	}

	msg, err := scope.TopicReader().ReadMessage(scope.Ctx)
	scope.Require.NoError(err)

	firstPartitionID := msg.PartitionID()
	messagesCounter.Add(-1)

	t.Log("Read first partition id:", firstPartitionID)

	readFromFirstPartition := 1
readFromFirstPartition:
	for {
		msg, err = scope.TopicReader().ReadMessage(scope.Ctx)
		scope.Require.NoError(err)
		messagesCounter.Add(-1)
		readFromFirstPartition++
		if msg.PartitionID() != firstPartitionID {
			t.Log("Read first partition id:", msg.PartitionID(), "read from first partition:", readFromFirstPartition)
			break readFromFirstPartition
		}
	}

	writersStop.Store(true)
	writers.Wait()
	t.Log("Writers stopped, need to read messages: ", messagesCounter.Load())

	for messagesCounter.Load() > 0 {
		msg, err = scope.TopicReader().ReadMessage(scope.Ctx)
		scope.Require.NoError(err)
		scope.Require.NotEqual(firstPartitionID, msg.PartitionID())
		messagesCounter.Add(-1)
	}
}
