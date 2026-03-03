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

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func readMessages(ctx context.Context, count int, topicPath string, scope *scopeT) error {
	reader, err := scope.Driver().Topic().StartReader(
		consumerName,
		topicoptions.ReadTopic(topicPath),
	)
	if err != nil {
		return err
	}

	partitionsSeqNoMap := make(map[int64][]int64)

	for i := range count {
		readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		mess, err := reader.ReadMessage(readCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if i != count-1 {
					return fmt.Errorf("not all messages read: %d, expected: %d", i, count)
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

// CreateTopicWithAutoPartitioning creates a topic with auto partitioning.
func createTopicWithAutoPartitioning(ctx context.Context, db *ydb.Driver, topicPath string) error {
	return db.Topic().Create(
		ctx,
		topicPath,
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw),
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName}),
		// MinActivePartitions(2), MaxActivePartitions(100)
		topicoptions.CreateWithMinActivePartitions(2),
		topicoptions.CreateWithMaxActivePartitions(100),
		// AutoPartitioningSettings: Up=2, Down=1, Window=2s, Strategy=ScaleUp
		topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
			AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
			AutoPartitioningWriteSpeedStrategy: topictypes.AutoPartitioningWriteSpeedStrategy{
				UpUtilizationPercent:   2,
				DownUtilizationPercent: 1,
				StabilizationWindow:    2 * time.Second,
			},
		}),
	)
}

func createMultiWriterForAutoPartitioning(
	t *testing.T,
	producerIDPrefix string,
	ctx context.Context,
	topicPath string,
	topicClient topic.Client,
	multiWriterSettings []topicoptions.MultiWriterOption,
) *topicwriter.Writer {
	t.Helper()

	multiWriterSettings = append(multiWriterSettings, topicoptions.WithProducerIDPrefix(producerIDPrefix))
	multiWriter, err := topicClient.StartWriter(
		topicPath,
		topicoptions.WithWriterSetAutoSeqNo(false),
		topicoptions.WithMultiWriter(multiWriterSettings...),
	)
	require.NoError(t, err)
	require.NoError(t, multiWriter.WaitInit(ctx))
	return multiWriter
}

// TestTopicMultiWriter_WaitInitAndClose verifies that internal topic multi writer
// can be initialized and closed against a real YDB topic.
func TestTopicMultiWriter_WaitInitAndClose(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	multiWriter, err := topicClient.StartWriter(scope.TopicPath(), topicoptions.WithMultiWriter())
	require.NoError(t, err)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Close(ctx))
}

func TestTopicMultiWriter_WaitInitInfoUnimplemented(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	multiWriter, err := topicClient.StartWriter(scope.TopicPath(), topicoptions.WithMultiWriter())
	require.NoError(t, err)

	_, err = multiWriter.WaitInitInfo(ctx)
	require.ErrorIs(t, err, topicwriter.ErrUnimplemented)
}

// TestTopicMultiWriter_WaitInitAndClose verifies that internal topic multi writer
// can be initialized and closed against a real YDB topic.
func TestTopicMultiWriter_CloseWithoutWaitInit(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	multiWriter, err := topicClient.StartWriter(scope.TopicPath(), topicoptions.WithMultiWriter())
	require.NoError(t, err)

	require.NoError(t, multiWriter.Close(ctx))
}

func TestTopicMultiWriter_WriteAndFlush(t *testing.T) {
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

	multiWriter, err := topicClient.StartWriter(
		topicPath,
		topicoptions.WithWriterSetAutoSeqNo(false),
		topicoptions.WithMultiWriter(
			topicoptions.WithPartitionChooserStrategy(topicoptions.PartitionChooserStrategyHash),
		),
	)
	require.NoError(t, err)

	err = multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]topicwriter.Message, 0, 1000)
	for i := range 1000 {
		messages = append(messages, topicwriter.Message{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: int64(i + 1),
			Key:   fmt.Sprintf("partition-key-%d", i),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages...))
	require.NoError(t, multiWriter.Close(ctx))

	require.NoError(t, readMessages(ctx, 1000, topicPath, scope))
}

func TestTopicMultiWriter_AutoPartitioning(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	db := scope.Driver()
	topicClient := db.Topic()

	topicPath := db.Name() + "/" + t.Name() + "--auto-part-topic"
	_ = topicClient.Drop(ctx, topicPath)
	require.NoError(t, createTopicWithAutoPartitioning(ctx, db, topicPath))

	describe, err := topicClient.Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Len(t, describe.Partitions, 2)
	if len(describe.Partitions[0].FromBound) == 0 && len(describe.Partitions[0].ToBound) == 0 {
		t.Skip("skipping test because autosplit does not work in this version of YDB")
	}

	topicMultiWriterSettings := []topicoptions.MultiWriterOption{
		topicoptions.WithPartitionChooserStrategy(topicoptions.PartitionChooserStrategyBound),
		topicoptions.WithWriterIdleTimeout(30 * time.Second),
	}

	multiWriter1 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_1", ctx, topicPath, topicClient, topicMultiWriterSettings)
	multiWriter2 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_2", ctx, topicPath, topicClient, topicMultiWriterSettings)

	msgData := bytes.Repeat([]byte{'a'}, 1<<20) // 1 MB
	keys := make([]string, 0, len(describe.Partitions))
	for _, p := range describe.Partitions {
		keys = append(keys, string(p.FromBound))
	}
	require.NotEmpty(t, keys)

	writeMessage := func(m *topicwriter.Writer, payload []byte, seqNo int64) {
		key := keys[seqNo%int64(len(keys))]
		if key == "" {
			key = "lalala"
		}

		msg := topicwriter.Message{
			Data:  bytes.NewReader(payload),
			SeqNo: seqNo,
			Key:   key,
		}

		require.NoError(t, m.Write(ctx, msg))
	}

	writeMessage(multiWriter1, msgData, 1)
	writeMessage(multiWriter1, msgData, 2)
	time.Sleep(5 * time.Second)

	describe, err = topicClient.Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Len(t, describe.Partitions, 2)

	writeMessage(multiWriter1, msgData, 3)
	writeMessage(multiWriter1, msgData, 4)
	writeMessage(multiWriter1, msgData, 5)
	writeMessage(multiWriter1, msgData, 6)
	writeMessage(multiWriter1, msgData, 7)
	writeMessage(multiWriter2, msgData, 8)
	writeMessage(multiWriter1, msgData, 9)
	writeMessage(multiWriter1, msgData, 10)
	writeMessage(multiWriter2, msgData, 11)
	writeMessage(multiWriter1, msgData, 12)

	require.NoError(t, multiWriter1.Flush(ctx))
	require.NoError(t, multiWriter2.Flush(ctx))
	time.Sleep(5 * time.Second)

	describeResult, err := topicClient.Describe(ctx, topicPath)
	require.NoError(t, err)
	partitionsCount := len(describeResult.Partitions)
	require.GreaterOrEqual(t, partitionsCount, 4, "partitions count: %d, expected at least 4", partitionsCount)

	writeMessage(multiWriter1, msgData, 13)
	writeMessage(multiWriter1, msgData, 14)
	require.NoError(t, multiWriter1.Flush(ctx))
	require.NoError(t, multiWriter2.Flush(ctx))

	multiWriter3 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_3", ctx, topicPath, topicClient, topicMultiWriterSettings)

	require.NoError(t, multiWriter3.Close(ctx))
	require.NoError(t, multiWriter1.Close(ctx))
	require.NoError(t, multiWriter2.Close(ctx))
}
