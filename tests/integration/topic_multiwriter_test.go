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
	const partitionWriteSpeed = 1 << 20

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
		topicoptions.CreateWithPartitionWriteSpeedBytesPerSecond(partitionWriteSpeed),
		topicoptions.CreateWithPartitionWriteBurstBytes(partitionWriteSpeed),
	)
}

func createMultiWriterForAutoPartitioning(
	t *testing.T,
	producerIDPrefix string,
	ctx context.Context,
	topicPath string,
	topicClient topic.Client,
	writerOptions []topicoptions.WriterOption,
) (*topicwriter.Writer, int64) {
	t.Helper()

	writerOptions = append(writerOptions, topicoptions.WithWriterSetAutoSeqNo(false))
	writerOptions = append(writerOptions, topicoptions.WithProducerIDPrefix(producerIDPrefix))
	multiWriter, err := topicClient.StartWriter(
		topicPath,
		writerOptions...,
	)
	require.NoError(t, err)
	lastSeqNo, err := multiWriter.WaitInitInfo(ctx)
	require.NoError(t, err)
	return multiWriter, lastSeqNo.LastSeqNum
}

// TestTopicMultiWriter_WaitInitAndClose verifies that internal topic multi writer
// can be initialized and closed against a real YDB topic.
func TestTopicMultiWriter_WaitInitAndClose(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	multiWriter, err := topicClient.StartWriter(scope.TopicPath(), topicoptions.WithProducerIDPrefix("test-producer"))
	require.NoError(t, err)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Close(ctx))
}

// TestTopicMultiWriter_WaitInitAndClose verifies that internal topic multi writer
// can be initialized and closed against a real YDB topic.
func TestTopicMultiWriter_CloseWithoutWaitInit(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	multiWriter, err := topicClient.StartWriter(
		scope.TopicPath(),
		topicoptions.WithPartitionChooserStrategy(topicoptions.PartitionChooserStrategyHash),
		topicoptions.WithProducerIDPrefix("test-producer"),
	)
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
		topicoptions.WithPartitionChooserStrategy(topicoptions.PartitionChooserStrategyHash),
		topicoptions.WithProducerIDPrefix("test-producer"),
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

func TestTopicMultiWriter_WithDefaultSettings(t *testing.T) {
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
		topicoptions.WithWriterPartitionByKey(),
	)
	require.NoError(t, err)

	err = multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]topicwriter.Message, 0, 1000)
	for i := range 1000 {
		messages = append(messages, topicwriter.Message{
			Data: bytes.NewReader([]byte("hello")),
			Key:  fmt.Sprintf("partition-key-%d", i),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages...))
	require.NoError(t, multiWriter.Close(ctx))

	require.NoError(t, readMessages(ctx, 1000, topicPath, scope))
}

func TestTopicMultiWriter_WithPartitionIDInMessage(t *testing.T) {
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
		topicoptions.WithWriterPartitionByPartitionID(),
	)
	require.NoError(t, err)

	err = multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	describe, err := topicClient.Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Len(t, describe.Partitions, 10)

	partitionIDs := make([]int64, 0, len(describe.Partitions))
	for _, p := range describe.Partitions {
		partitionIDs = append(partitionIDs, p.PartitionID)
	}

	messages := make([]topicwriter.Message, 0, 1000)
	for i := range 1000 {
		messages = append(messages, topicwriter.Message{
			Data:        bytes.NewReader([]byte("hello")),
			PartitionID: partitionIDs[i%len(partitionIDs)],
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages...))
	require.NoError(t, multiWriter.Close(ctx))

	require.NoError(t, readMessages(ctx, 1000, topicPath, scope))
}

func TestTopicMultiWriter_AutoPartitioning(t *testing.T) {
	const firstPartitionKey = "__first_partition__"

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

	topicMultiWriterSettings := []topicoptions.WriterOption{
		topicoptions.WithPartitionChooserStrategy(topicoptions.PartitionChooserStrategyBound),
		topicoptions.WithWriterIdleTimeout(30 * time.Second),
		topicoptions.WithPartitioningKeyHasher(func(key string) string {
			if key == firstPartitionKey {
				return ""
			}

			return key
		}),
	}

	multiWriter1, nextSeqNo1 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_1", ctx, topicPath, topicClient, topicMultiWriterSettings)
	multiWriter2, nextSeqNo2 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_2", ctx, topicPath, topicClient, topicMultiWriterSettings)
	msgData := bytes.Repeat([]byte{'a'}, 1<<20) // 1 MB

	getKeys := func(partitions []topictypes.PartitionInfo) []string {
		keys := make([]string, 0, len(partitions))
		for _, p := range partitions {
			if len(p.FromBound) == 0 {
				keys = append(keys, firstPartitionKey)

				continue
			}

			keys = append(keys, string(p.FromBound))
		}

		return keys
	}

	require.NotEmpty(t, getKeys(describe.Partitions))

	writeMessage := func(m *topicwriter.Writer, payload []byte, seqNo int64, key string) {
		msg := topicwriter.Message{
			Data:  bytes.NewReader(payload),
			SeqNo: seqNo,
			Key:   key,
		}

		require.NoError(t, m.Write(ctx, msg))
	}

	nextSeqNo1++
	nextSeqNo2++

	writeLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range getKeys(partitions) {
			writeMessage(multiWriter1, msgData, nextSeqNo1, key)
			nextSeqNo1++
			writeMessage(multiWriter2, msgData, nextSeqNo2, key)
			nextSeqNo2++
		}

		require.NoError(t, multiWriter1.Flush(ctx))
		require.NoError(t, multiWriter2.Flush(ctx))
	}

	waitForPartitionsCountAtLeast := func(minCount int, timeout time.Duration) topictypes.TopicDescription {
		deadline := time.Now().Add(timeout)
		nextLoadAt := time.Now()
		for {
			describeResult, describeErr := topicClient.Describe(ctx, topicPath)
			require.NoError(t, describeErr)
			if len(describeResult.Partitions) >= minCount {
				return describeResult
			}

			if time.Now().After(deadline) {
				t.Fatalf("partitions count: %d, expected at least %d", len(describeResult.Partitions), minCount)
			}

			// Keep producing traffic while waiting so auto-partitioning has sustained load to react to.
			if time.Now().After(nextLoadAt) {
				writeLoadRound(describeResult.Partitions)
				nextLoadAt = time.Now().Add(500 * time.Millisecond)
			}

			time.Sleep(250 * time.Millisecond)
		}
	}

	writeLoadRound(describe.Partitions)
	writeLoadRound(describe.Partitions)

	describe = waitForPartitionsCountAtLeast(3, 20*time.Second)

	writeLoadRound(describe.Partitions)
	writeLoadRound(describe.Partitions)

	describe = waitForPartitionsCountAtLeast(4, 20*time.Second)
	require.GreaterOrEqual(t, len(describe.Partitions), 4, "partitions count: %d, expected at least 4", len(describe.Partitions))

	writeMessage(multiWriter1, msgData, nextSeqNo1, getKeys(describe.Partitions)[0])
	nextSeqNo1++
	writeMessage(multiWriter1, msgData, nextSeqNo1, getKeys(describe.Partitions)[0])
	require.NoError(t, multiWriter1.Flush(ctx))
	require.NoError(t, multiWriter2.Flush(ctx))

	multiWriter3, _ := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_3", ctx, topicPath, topicClient, topicMultiWriterSettings)

	require.NoError(t, multiWriter3.Close(ctx))
	require.NoError(t, multiWriter1.Close(ctx))
	require.NoError(t, multiWriter2.Close(ctx))
}
