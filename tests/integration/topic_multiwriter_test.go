//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type partitionProducerKey struct {
	partitionID int64
	producerID  string
}

const (
	defaultMessageCount  = 1000
	defaultReadTimeout   = 20 * time.Second
	defaultMessageString = "hello"
)

func writeAndReadMessages(
	t testing.TB,
	ctx context.Context,
	topicClient topic.Client,
	topicPath string,
	multiWriter *topicwriter.Writer,
	makeMessage func(i int) topicwriter.Message,
) {
	t.Helper()

	messages := make([]topicwriter.Message, 0, defaultMessageCount)
	for i := range defaultMessageCount {
		messages = append(messages, makeMessage(i))
	}

	require.NoError(t, multiWriter.Write(ctx, messages...))
	require.NoError(t, multiWriter.Close(ctx))

	require.NoError(
		t,
		readMessagesAndAssertOrderedBySeqNo(
			ctx,
			topicClient,
			topicPath,
			consumerName,
			defaultMessageCount,
			defaultReadTimeout,
			[]byte(defaultMessageString),
		),
	)
}

// readMessagesAndAssertOrderedBySeqNo reads exactly expectedCount messages from the topic and asserts that
// within each (partitionID, producerID) pair seqNo is strictly increasing.
func readMessagesAndAssertOrderedBySeqNo(
	ctx context.Context,
	client topic.Client,
	topicPath string,
	consumerName string,
	expectedCount int,
	timeout time.Duration,
	expectedPayload []byte,
) error {
	reader, err := client.StartReader(
		consumerName,
		topicoptions.ReadSelectors{
			{
				Path:     topicPath,
				ReadFrom: time.Unix(0, 0).UTC(),
			},
		},
		topicoptions.WithReaderSupportSplitMergePartitions(true),
	)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close(context.Background())
	}()

	type message struct {
		partitionID int64
		producerID  string
		seqNo       int64
		payload     []byte
	}

	messages := make([]message, 0, expectedCount)
	deadline := time.Now().Add(timeout)

	for len(messages) < expectedCount {
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"expected to read %d messages within %s, got %d",
				expectedCount,
				timeout,
				len(messages),
			)
		}

		readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		msg, err := reader.ReadMessage(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Retry until overall timeout is reached.
				continue
			}

			return err
		}

		payload := make([]byte, msg.UncompressedSize)
		_, err = msg.Read(payload)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		messages = append(messages, message{
			partitionID: msg.PartitionID(),
			producerID:  msg.ProducerID,
			seqNo:       msg.SeqNo,
			payload:     payload,
		})
	}

	if len(messages) != expectedCount {
		return fmt.Errorf(
			"read message count mismatch: got %d, expected %d",
			len(messages),
			expectedCount,
		)
	}

	byPartitionAndProducer := make(map[partitionProducerKey][]int64)
	for _, m := range messages {
		key := partitionProducerKey{
			partitionID: m.partitionID,
			producerID:  m.producerID,
		}
		byPartitionAndProducer[key] = append(byPartitionAndProducer[key], m.seqNo)
		if !bytes.Equal(expectedPayload, m.payload) {
			return fmt.Errorf(
				"partition %d, producerId %s: payload mismatch, expected %d bytes, got %d bytes, seqNo %d",
				key.partitionID,
				key.producerID,
				len(expectedPayload),
				len(m.payload),
				m.seqNo,
			)
		}
	}

	for key, seqNos := range byPartitionAndProducer {
		for i := 1; i < len(seqNos); i++ {
			if seqNos[i] <= seqNos[i-1] {
				return fmt.Errorf(
					"partition %d, producerId %s: expected seqNo strictly increasing, got %d then %d at index %d",
					key.partitionID,
					key.producerID,
					seqNos[i-1],
					seqNos[i],
					i,
				)
			}
		}
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
	t testing.TB,
	producerIDPrefix string,
	ctx context.Context,
	topicPath string,
	topicClient topic.Client,
	multiWriterOptions []topicoptions.MultiWriterOption,
) *topicwriter.Writer {
	t.Helper()

	writerOptions := []topicoptions.WriterOption{
		topicoptions.WithWriterSetAutoSeqNo(true),
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithProducerIDPrefix(producerIDPrefix),
		),
	}

	multiWriter, err := topicClient.StartWriter(
		topicPath,
		writerOptions...,
	)
	require.NoError(t, err)
	err = multiWriter.WaitInit(ctx)
	require.NoError(t, err)
	return multiWriter
}

// TestTopicMultiWriter_WaitInitAndClose verifies that internal topic multi writer
// can be initialized and closed against a real YDB topic.
func TestTopicMultiWriter_WaitInitAndClose(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	multiWriter, err := topicClient.StartWriter(scope.TopicPath(), topicoptions.WithWriteToManyPartitions(
		topicoptions.WithProducerIDPrefix("test-producer"),
	))
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
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
			topicoptions.WithProducerIDPrefix("test-producer"),
		),
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
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
			topicoptions.WithProducerIDPrefix("test-producer"),
		),
	)
	require.NoError(t, err)

	err = multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	writeAndReadMessages(
		t,
		ctx,
		topicClient,
		topicPath,
		multiWriter,
		func(i int) topicwriter.Message {
			return topicwriter.Message{
				Data:  bytes.NewReader([]byte(defaultMessageString)),
				SeqNo: int64(i + 1),
				Key:   fmt.Sprintf("partition-key-%d", i),
			}
		},
	)
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
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
		),
	)
	require.NoError(t, err)

	err = multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	writeAndReadMessages(
		t,
		ctx,
		topicClient,
		topicPath,
		multiWriter,
		func(i int) topicwriter.Message {
			return topicwriter.Message{
				Data: bytes.NewReader([]byte(defaultMessageString)),
				Key:  fmt.Sprintf("partition-key-%d", i),
			}
		},
	)
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
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByPartitionID(),
		),
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

	writeAndReadMessages(
		t,
		ctx,
		topicClient,
		topicPath,
		multiWriter,
		func(i int) topicwriter.Message {
			return topicwriter.Message{
				Data:        bytes.NewReader([]byte(defaultMessageString)),
				PartitionID: partitionIDs[i%len(partitionIDs)],
			}
		},
	)
}

func runTestWithAutoPartitioning(t testing.TB, scope *scopeT) {
	const firstPartitionKey = "__first_partition__"

	ctx := scope.Ctx

	db := scope.Driver()
	topicClient := db.Topic()

	topicPath := db.Name() + "/" + t.Name() + "--auto-part-topic--" + uuid.NewString()
	_ = topicClient.Drop(ctx, topicPath)
	require.NoError(t, createTopicWithAutoPartitioning(ctx, db, topicPath))

	describe, err := topicClient.Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Len(t, describe.Partitions, 2)
	if len(describe.Partitions[0].FromBound) == 0 && len(describe.Partitions[0].ToBound) == 0 {
		t.Skip("skipping test because autosplit does not work in this version of YDB")
	}

	topicMultiWriterSettings := []topicoptions.MultiWriterOption{
		topicoptions.WithWriterPartitionByKey(
			topicoptions.BoundPartitionChooser(
				topicoptions.WithBoundPartitionChooserPartitioningKeyHasher(
					func(key string) string {
						if key == firstPartitionKey {
							return ""
						}

						return key
					},
				),
			),
		),
		topicoptions.WithWriterIdleTimeout(30 * time.Second),
	}

	multiWriter1 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_1", ctx, topicPath, topicClient, topicMultiWriterSettings)
	multiWriter2 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_2", ctx, topicPath, topicClient, topicMultiWriterSettings)
	msgData := bytes.Repeat([]byte{'a'}, 1<<20) // 1 MB

	var messagesWritten1, messagesWritten2 int

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

	writeMessage := func(m *topicwriter.Writer, payload []byte, key string) {
		msg := topicwriter.Message{
			Data: bytes.NewReader(payload),
			Key:  key,
		}

		require.NoError(t, m.Write(ctx, msg))
	}

	writeLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range getKeys(partitions) {
			writeMessage(multiWriter1, msgData, key)
			messagesWritten1++
			writeMessage(multiWriter2, msgData, key)
			messagesWritten2++
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

	writeMessage(multiWriter1, msgData, getKeys(describe.Partitions)[0])
	writeMessage(multiWriter1, msgData, getKeys(describe.Partitions)[0])
	messagesWritten1 += 2
	require.NoError(t, multiWriter1.Flush(ctx))
	require.NoError(t, multiWriter2.Flush(ctx))

	multiWriter3 := createMultiWriterForAutoPartitioning(t, "autopartitioning_keyed_3", ctx, topicPath, topicClient, topicMultiWriterSettings)

	require.NoError(t, multiWriter3.Close(ctx))
	require.NoError(t, multiWriter1.Close(ctx))
	require.NoError(t, multiWriter2.Close(ctx))

	require.NoError(t, readMessagesAndAssertOrderedBySeqNo(
		ctx,
		topicClient,
		topicPath,
		consumerName,
		messagesWritten1+messagesWritten2,
		20*time.Second,
		msgData,
	))
}

type customPartitionChooser struct{}

// ChoosePartition implements [topicmultiwriter.PartitionChooser].
func (c *customPartitionChooser) ChoosePartition(msg topicwriter.Message) (int64, error) {
	return 1, nil
}

// AddNewPartitions implements [topicmultiwriter.PartitionChooser].
func (c *customPartitionChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	return nil
}

// RemovePartition implements [topicmultiwriter.PartitionChooser].
func (c *customPartitionChooser) RemovePartition(partitionID int64) {

}

func TestTopicMultiWriter_WithCustomPartitioning(t *testing.T) {
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
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByKey(&customPartitionChooser{}),
		),
	)
	require.NoError(t, err)

	err = multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	writeAndReadMessages(
		t,
		ctx,
		topicClient,
		topicPath,
		multiWriter,
		func(i int) topicwriter.Message {
			return topicwriter.Message{
				Data: bytes.NewReader([]byte(defaultMessageString)),
			}
		},
	)
}

func TestTopicMultiWriter_AutoPartitioning(t *testing.T) {
	scope := newScope(t)
	xtest.TestManyTimes(
		t,
		func(t testing.TB) {
			runTestWithAutoPartitioning(t, scope)
		},
		xtest.StopAfter(40*time.Second),
	)
}
