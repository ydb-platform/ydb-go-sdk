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

type receivedTopicMessage struct {
	partitionID int64
	producerID  string
	seqNo       int64
	payload     []byte
	metadata    map[string][]byte
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

func cloneBytesMetadata(src map[string][]byte) map[string][]byte {
	if src == nil {
		return nil
	}

	dst := make(map[string][]byte, len(src))
	for k, v := range src {
		dst[k] = bytes.Clone(v)
	}

	return dst
}

func readMessagesDetailed(
	ctx context.Context,
	client topic.Client,
	topicPath string,
	consumerName string,
	expectedCount int,
	timeout time.Duration,
) ([]receivedTopicMessage, error) {
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
		return nil, err
	}
	defer func() {
		_ = reader.Close(context.Background())
	}()

	messages := make([]receivedTopicMessage, 0, expectedCount)
	deadline := time.Now().Add(timeout)

	for len(messages) < expectedCount {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf(
				"expected to read %d messages within %s, got %d",
				expectedCount,
				timeout,
				len(messages),
			)
		}

		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := reader.ReadMessage(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			return nil, err
		}

		payload := make([]byte, msg.UncompressedSize)
		_, err = msg.Read(payload)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		messages = append(messages, receivedTopicMessage{
			partitionID: msg.PartitionID(),
			producerID:  msg.ProducerID,
			seqNo:       msg.SeqNo,
			payload:     payload,
			metadata:    cloneBytesMetadata(msg.Metadata),
		})
	}

	return messages, nil
}

func assertMessagesOrderedBySeqNo(messages []receivedTopicMessage) error {
	byPartitionAndProducer := make(map[partitionProducerKey][]int64)
	for _, m := range messages {
		key := partitionProducerKey{
			partitionID: m.partitionID,
			producerID:  m.producerID,
		}
		byPartitionAndProducer[key] = append(byPartitionAndProducer[key], m.seqNo)
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
	messages, err := readMessagesDetailed(ctx, client, topicPath, consumerName, expectedCount, timeout)
	if err != nil {
		return err
	}
	for _, m := range messages {
		if !bytes.Equal(expectedPayload, m.payload) {
			return fmt.Errorf(
				"partition %d, producerId %s: payload mismatch, expected %d bytes, got %d bytes, seqNo %d",
				m.partitionID,
				m.producerID,
				len(expectedPayload),
				len(m.payload),
				m.seqNo,
			)
		}
	}

	return assertMessagesOrderedBySeqNo(messages)
}

// readPartitionByPayload reads from the topic until each distinct payload in wantPayloads has been
// observed at least once, and returns the partition ID of the first message with that payload.
func readPartitionByPayload(
	ctx context.Context,
	client topic.Client,
	topicPath string,
	wantPayloads [][]byte,
	timeout time.Duration,
) (map[string]int64, error) {
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
		return nil, err
	}
	defer func() {
		_ = reader.Close(context.Background())
	}()

	wantSet := make(map[string]struct{}, len(wantPayloads))
	for _, p := range wantPayloads {
		wantSet[string(p)] = struct{}{}
	}

	found := make(map[string]int64, len(wantSet))
	deadline := time.Now().Add(timeout)

	for len(found) < len(wantSet) {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf(
				"readPartitionByPayload: timeout after %s, found %d/%d payloads",
				timeout,
				len(found),
				len(wantSet),
			)
		}

		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := reader.ReadMessage(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			return nil, err
		}

		payload := make([]byte, msg.UncompressedSize)
		_, err = msg.Read(payload)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		ps := string(payload)
		if _, want := wantSet[ps]; !want {
			continue
		}

		if _, ok := found[ps]; !ok {
			found[ps] = msg.PartitionID()
		}
	}

	return found, nil
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
	firstPartitionKey string,
) *topicwriter.Writer {
	t.Helper()

	multiWriterOptions := []topicoptions.MultiWriterOption{
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
		topicoptions.WithProducerIDPrefix(producerIDPrefix),
	}

	writerOptions := []topicoptions.WriterOption{
		topicoptions.WithWriterSetAutoSeqNo(true),
		topicoptions.WithWriteToManyPartitions(
			multiWriterOptions...,
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

func createKafkaHashMultiWriter(
	t testing.TB,
	ctx context.Context,
	topicClient topic.Client,
	topicPath string,
	producerIDPrefix string,
) *topicwriter.Writer {
	t.Helper()

	writer, err := topicClient.StartWriter(
		topicPath,
		topicoptions.WithWriterSetAutoSeqNo(true),
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
			topicoptions.WithProducerIDPrefix(producerIDPrefix),
		),
	)
	require.NoError(t, err)
	require.NoError(t, writer.WaitInit(ctx))

	return writer
}

type autoPartitioningScenario struct {
	t                 testing.TB
	ctx               context.Context
	topicClient       topic.Client
	topicPath         string
	firstPartitionKey string
	writer1           *topicwriter.Writer
	writer2           *topicwriter.Writer
}

func newAutoPartitioningScenario(
	t testing.TB,
	scope *scopeT,
	topicPathMarker string,
	producerStem string,
) (*autoPartitioningScenario, topictypes.TopicDescription) {
	t.Helper()

	const firstPartitionKey = "__first_partition__"

	ctx := scope.Ctx
	db := scope.Driver()
	topicClient := db.Topic()

	topicPath := db.Name() + "/" + t.Name() + topicPathMarker + uuid.NewString()
	_ = topicClient.Drop(ctx, topicPath)
	require.NoError(t, createTopicWithAutoPartitioning(ctx, db, topicPath))

	describe, err := topicClient.Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Len(t, describe.Partitions, 2)
	if len(describe.Partitions[0].FromBound) == 0 && len(describe.Partitions[0].ToBound) == 0 {
		t.Skip("skipping test because autosplit does not work in this version of YDB")
	}

	scenario := &autoPartitioningScenario{
		t:                 t,
		ctx:               ctx,
		topicClient:       topicClient,
		topicPath:         topicPath,
		firstPartitionKey: firstPartitionKey,
		writer1:           createMultiWriterForAutoPartitioning(t, fmt.Sprintf("%s_1", producerStem), ctx, topicPath, topicClient, firstPartitionKey),
		writer2:           createMultiWriterForAutoPartitioning(t, fmt.Sprintf("%s_2", producerStem), ctx, topicPath, topicClient, firstPartitionKey),
	}

	return scenario, describe
}

func (s *autoPartitioningScenario) getKeys(partitions []topictypes.PartitionInfo) []string {
	keys := make([]string, 0, len(partitions))
	for _, p := range partitions {
		if len(p.FromBound) == 0 {
			keys = append(keys, s.firstPartitionKey)

			continue
		}

		keys = append(keys, string(p.FromBound))
	}

	return keys
}

func (s *autoPartitioningScenario) writeMessages(
	writer *topicwriter.Writer,
	payload []byte,
	key string,
	metadata map[string][]byte,
	count int,
) int {
	s.t.Helper()
	if count <= 0 {
		count = 1
	}

	messages := make([]topicwriter.Message, 0, count)
	for range count {
		messages = append(messages, topicwriter.Message{
			Data:     bytes.NewReader(payload),
			Key:      key,
			Metadata: cloneBytesMetadata(metadata),
		})
	}

	require.NoError(s.t, writer.Write(s.ctx, messages...))

	return len(messages)
}

func (s *autoPartitioningScenario) flushBoth(ctx context.Context) {
	s.t.Helper()
	require.NoError(s.t, s.writer1.Flush(ctx))
	require.NoError(s.t, s.writer2.Flush(ctx))
}

func (s *autoPartitioningScenario) closeAll(ctx context.Context, extraWriters ...*topicwriter.Writer) {
	s.t.Helper()
	for _, writer := range extraWriters {
		require.NoError(s.t, writer.Close(ctx))
	}
	require.NoError(s.t, s.writer1.Close(ctx))
	require.NoError(s.t, s.writer2.Close(ctx))
}

func (s *autoPartitioningScenario) waitForPartitionsCountAtLeast(
	minCount int,
	timeout time.Duration,
	onTick func(partitions []topictypes.PartitionInfo),
) topictypes.TopicDescription {
	s.t.Helper()

	deadline := time.Now().Add(timeout)
	nextActionAt := time.Now()

	for {
		describeResult, describeErr := s.topicClient.Describe(s.ctx, s.topicPath)
		require.NoError(s.t, describeErr)
		if len(describeResult.Partitions) >= minCount {
			return describeResult
		}

		if time.Now().After(deadline) {
			s.t.Fatalf("partitions count: %d, expected at least %d", len(describeResult.Partitions), minCount)
		}

		if onTick != nil && time.Now().After(nextActionAt) {
			onTick(describeResult.Partitions)
			nextActionAt = time.Now().Add(500 * time.Millisecond)
		}

		time.Sleep(250 * time.Millisecond)
	}
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

// TestTopicMultiWriter_KafkaHashPartitionStableAcrossWriterRestart checks that with Kafka-style hash
// partitioning, the same message key maps to the same partition after closing the writer and
// starting a new one (simulating application restart).
func TestTopicMultiWriter_KafkaHashPartitionStableAcrossWriterRestart(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()

	topicPath := createTopic(ctx, t, scope.Driver())
	err := topicClient.Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithMinActivePartitions(10),
		topicoptions.AlterWithMaxActivePartitions(10),
	)
	require.NoError(t, err)

	const (
		producerPrefix = "kafka-hash-restart-producer"
		stableKey      = "stable-partitioning-key-restart-42"
	)
	payloadFirst := []byte("payload-kafka-hash-first-session")
	payloadSecond := []byte("payload-kafka-hash-second-session")

	multiWriterOpts := []topicoptions.MultiWriterOption{
		topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
		topicoptions.WithProducerIDPrefix(producerPrefix),
	}

	startWriter := func() *topicwriter.Writer {
		w, wErr := topicClient.StartWriter(
			topicPath,
			topicoptions.WithWriterSetAutoSeqNo(true),
			topicoptions.WithWriteToManyPartitions(multiWriterOpts...),
		)
		require.NoError(t, wErr)
		require.NoError(t, w.WaitInit(ctx))

		return w
	}

	w1 := startWriter()
	require.NoError(t, w1.Write(ctx, topicwriter.Message{
		Data: bytes.NewReader(payloadFirst),
		Key:  stableKey,
	}))
	require.NoError(t, w1.Flush(ctx))
	require.NoError(t, w1.Close(ctx))

	w2 := startWriter()
	require.NoError(t, w2.Write(ctx, topicwriter.Message{
		Data: bytes.NewReader(payloadSecond),
		Key:  stableKey,
	}))
	require.NoError(t, w2.Flush(ctx))
	require.NoError(t, w2.Close(ctx))

	partitionsByPayload, err := readPartitionByPayload(
		ctx,
		topicClient,
		topicPath,
		[][]byte{payloadFirst, payloadSecond},
		defaultReadTimeout,
	)
	require.NoError(t, err)

	require.Equal(
		t,
		partitionsByPayload[string(payloadFirst)],
		partitionsByPayload[string(payloadSecond)],
		"same key must map to the same partition after writer restart (Kafka hash strategy)",
	)
}

func TestTopicMultiWriter_KafkaHashChooser_SameKeySamePartitionAcrossManyRestarts(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	topicPath := createTopic(ctx, t, scope.Driver())
	err := topicClient.Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithMinActivePartitions(10),
		topicoptions.AlterWithMaxActivePartitions(10),
	)
	require.NoError(t, err)

	const (
		restartCount   = 5
		producerPrefix = "kafka-hash-many-restarts"
		stableKey      = "same-key-across-many-restarts"
	)

	payloads := make([][]byte, 0, restartCount)
	for i := range restartCount {
		payload := []byte(fmt.Sprintf("payload-kafka-many-restarts-%d", i))
		payloads = append(payloads, payload)

		writer := createKafkaHashMultiWriter(t, ctx, topicClient, topicPath, producerPrefix)
		require.NoError(t, writer.Write(ctx, topicwriter.Message{
			Data: bytes.NewReader(payload),
			Key:  stableKey,
		}))
		require.NoError(t, writer.Flush(ctx))
		require.NoError(t, writer.Close(ctx))
	}

	partitionsByPayload, err := readPartitionByPayload(
		ctx,
		topicClient,
		topicPath,
		payloads,
		defaultReadTimeout,
	)
	require.NoError(t, err)

	require.Len(t, partitionsByPayload, restartCount)
	var expectedPartition int64
	for i, payload := range payloads {
		partitionID := partitionsByPayload[string(payload)]
		if i == 0 {
			expectedPartition = partitionID

			continue
		}

		require.Equal(t, expectedPartition, partitionID)
	}
}

func TestTopicMultiWriter_KafkaHashChooser_DifferentKeysDistributeAcrossPartitions(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	topicPath := createTopic(ctx, t, scope.Driver())
	err := topicClient.Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithMinActivePartitions(10),
		topicoptions.AlterWithMaxActivePartitions(10),
	)
	require.NoError(t, err)

	writer := createKafkaHashMultiWriter(t, ctx, topicClient, topicPath, "kafka-hash-distribution")

	const keyCount = 32
	payloads := make([][]byte, 0, keyCount)
	for i := range keyCount {
		payload := []byte(fmt.Sprintf("payload-kafka-distribution-%02d", i))
		payloads = append(payloads, payload)
		require.NoError(t, writer.Write(ctx, topicwriter.Message{
			Data: bytes.NewReader(payload),
			Key:  fmt.Sprintf("distribution-key-%02d", i),
		}))
	}
	require.NoError(t, writer.Close(ctx))

	partitionsByPayload, err := readPartitionByPayload(
		ctx,
		topicClient,
		topicPath,
		payloads,
		defaultReadTimeout,
	)
	require.NoError(t, err)

	uniquePartitions := make(map[int64]struct{})
	for _, payload := range payloads {
		uniquePartitions[partitionsByPayload[string(payload)]] = struct{}{}
	}

	require.Greater(t, len(uniquePartitions), 1, "different keys should not all map to one partition")
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

// autoPartitioningIntegrationVariant selects topic path fragment, per-message payload, and producer ID stem
// (writers use stem_1, stem_2, stem_3) for the shared auto-partitioning integration scenario.
// messagesPerLogicalWrite allows representing one "logical" write by several physical messages
// with the same key, e.g. to keep total written bytes equal across payload sizes.
type autoPartitioningIntegrationVariant struct {
	topicPathMarker         string
	payload                 []byte
	producerStem            string
	messagesPerLogicalWrite int
	readTimeout             time.Duration
}

func runTestWithAutoPartitioningVariant(t testing.TB, scope *scopeT, variant autoPartitioningIntegrationVariant) {
	const firstPartitionKey = "__first_partition__"
	if variant.messagesPerLogicalWrite <= 0 {
		variant.messagesPerLogicalWrite = 1
	}
	if variant.readTimeout <= 0 {
		variant.readTimeout = 20 * time.Second
	}

	ctx := scope.Ctx

	db := scope.Driver()
	topicClient := db.Topic()

	topicPath := db.Name() + "/" + t.Name() + variant.topicPathMarker + uuid.NewString()
	_ = topicClient.Drop(ctx, topicPath)
	require.NoError(t, createTopicWithAutoPartitioning(ctx, db, topicPath))

	describe, err := topicClient.Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Len(t, describe.Partitions, 2)
	if len(describe.Partitions[0].FromBound) == 0 && len(describe.Partitions[0].ToBound) == 0 {
		t.Skip("skipping test because autosplit does not work in this version of YDB")
	}

	producerID := func(i int) string {
		return fmt.Sprintf("%s_%d", variant.producerStem, i)
	}

	multiWriter1 := createMultiWriterForAutoPartitioning(t, producerID(1), ctx, topicPath, topicClient, firstPartitionKey)
	multiWriter2 := createMultiWriterForAutoPartitioning(t, producerID(2), ctx, topicPath, topicClient, firstPartitionKey)
	msgData := variant.payload

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

	writeMessage := func(m *topicwriter.Writer, payload []byte, key string) int {
		messages := make([]topicwriter.Message, 0, variant.messagesPerLogicalWrite)
		for range variant.messagesPerLogicalWrite {
			messages = append(messages, topicwriter.Message{
				Data: bytes.NewReader(payload),
				Key:  key,
			})
		}

		require.NoError(t, m.Write(ctx, messages...))

		return len(messages)
	}

	writeLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range getKeys(partitions) {
			messagesWritten1 += writeMessage(multiWriter1, msgData, key)
			messagesWritten2 += writeMessage(multiWriter2, msgData, key)
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

	messagesWritten1 += writeMessage(multiWriter1, msgData, getKeys(describe.Partitions)[0])
	messagesWritten1 += writeMessage(multiWriter1, msgData, getKeys(describe.Partitions)[0])
	require.NoError(t, multiWriter1.Flush(ctx))
	require.NoError(t, multiWriter2.Flush(ctx))

	multiWriter3 := createMultiWriterForAutoPartitioning(t, producerID(3), ctx, topicPath, topicClient, firstPartitionKey)

	require.NoError(t, multiWriter3.Close(ctx))
	require.NoError(t, multiWriter1.Close(ctx))
	require.NoError(t, multiWriter2.Close(ctx))

	require.NoError(t, readMessagesAndAssertOrderedBySeqNo(
		ctx,
		topicClient,
		topicPath,
		consumerName,
		messagesWritten1+messagesWritten2,
		variant.readTimeout,
		msgData,
	))
}

func runTestWithAutoPartitioning(t testing.TB, scope *scopeT) {
	runTestWithAutoPartitioningVariant(t, scope, autoPartitioningIntegrationVariant{
		topicPathMarker:         "--auto-part-topic--",
		payload:                 bytes.Repeat([]byte{'a'}, 1<<20), // 1 MiB
		producerStem:            "autopartitioning_keyed",
		messagesPerLogicalWrite: 1,
		readTimeout:             20 * time.Second,
	})
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
func (c *customPartitionChooser) RemovePartition(partitionID int64) {}

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
		xtest.StopAfter(60*time.Second),
	)
}

func TestTopicMultiWriter_AutoPartitioning_FlushDoesNotHangAfterSplit(t *testing.T) {
	scope := newScope(t)
	scenario, describe := newAutoPartitioningScenario(t, scope, "--auto-part-flush--", "autopartitioning_flush")
	payload := bytes.Repeat([]byte{'f'}, 1<<20)
	var written1, written2 int

	writeLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range scenario.getKeys(partitions) {
			written1 += scenario.writeMessages(scenario.writer1, payload, key, nil, 1)
			written2 += scenario.writeMessages(scenario.writer2, payload, key, nil, 1)
		}
		scenario.flushBoth(scenario.ctx)
	}

	writeLoadRound(describe.Partitions)
	writeLoadRound(describe.Partitions)
	_ = scenario.waitForPartitionsCountAtLeast(3, 20*time.Second, writeLoadRound)

	flushCtx, cancel := context.WithTimeout(scope.Ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, scenario.writer1.Flush(flushCtx))
	require.NoError(t, scenario.writer2.Flush(flushCtx))
	scenario.closeAll(scope.Ctx)

	require.NoError(t, readMessagesAndAssertOrderedBySeqNo(
		scope.Ctx,
		scenario.topicClient,
		scenario.topicPath,
		consumerName,
		written1+written2,
		30*time.Second,
		payload,
	))
}

func TestTopicMultiWriter_AutoPartitioning_CloseDoesNotHangAfterSplit(t *testing.T) {
	scope := newScope(t)
	scenario, describe := newAutoPartitioningScenario(t, scope, "--auto-part-close--", "autopartitioning_close")
	payload := bytes.Repeat([]byte{'c'}, 1<<20)
	var written1, written2 int

	writeLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range scenario.getKeys(partitions) {
			written1 += scenario.writeMessages(scenario.writer1, payload, key, nil, 1)
			written2 += scenario.writeMessages(scenario.writer2, payload, key, nil, 1)
		}
		scenario.flushBoth(scenario.ctx)
	}

	writeLoadRound(describe.Partitions)
	writeLoadRound(describe.Partitions)
	_ = scenario.waitForPartitionsCountAtLeast(3, 20*time.Second, writeLoadRound)

	closeCtx, cancel := context.WithTimeout(scope.Ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, scenario.writer1.Close(closeCtx))
	require.NoError(t, scenario.writer2.Close(closeCtx))

	require.NoError(t, readMessagesAndAssertOrderedBySeqNo(
		scope.Ctx,
		scenario.topicClient,
		scenario.topicPath,
		consumerName,
		written1+written2,
		30*time.Second,
		payload,
	))
}

func TestTopicMultiWriter_AutoPartitioning_MixedLargeAndSmallMessages(t *testing.T) {
	scope := newScope(t)
	scenario, describe := newAutoPartitioningScenario(t, scope, "--auto-part-mixed--", "autopartitioning_mixed")
	smallPayload := bytes.Repeat([]byte{'s'}, 64)
	largePayload := bytes.Repeat([]byte{'l'}, 1<<20)
	var (
		smallCount int
		largeCount int
		roundNo    int
	)

	writeMixedLoadRound := func(partitions []topictypes.PartitionInfo) {
		keys := scenario.getKeys(partitions)
		for i, key := range keys {
			payload := smallPayload
			if (roundNo+i)%10 == 0 {
				payload = largePayload
				largeCount += 2
			} else {
				smallCount += 2
			}
			scenario.writeMessages(scenario.writer1, payload, key, nil, 1)
			scenario.writeMessages(scenario.writer2, payload, key, nil, 1)
		}
		roundNo += len(keys)
		scenario.flushBoth(scenario.ctx)
	}

	writeMixedLoadRound(describe.Partitions)
	writeMixedLoadRound(describe.Partitions)
	describe = scenario.waitForPartitionsCountAtLeast(3, 20*time.Second, writeMixedLoadRound)
	writeMixedLoadRound(describe.Partitions)
	writeMixedLoadRound(describe.Partitions)
	scenario.closeAll(scope.Ctx)

	messages, err := readMessagesDetailed(
		scope.Ctx,
		scenario.topicClient,
		scenario.topicPath,
		consumerName,
		smallCount+largeCount,
		60*time.Second,
	)
	require.NoError(t, err)
	require.NoError(t, assertMessagesOrderedBySeqNo(messages))

	var readSmallCount, readLargeCount int
	for _, msg := range messages {
		switch {
		case bytes.Equal(msg.payload, smallPayload):
			readSmallCount++
		case bytes.Equal(msg.payload, largePayload):
			readLargeCount++
		default:
			t.Fatalf("unexpected payload size %d", len(msg.payload))
		}
	}

	require.Equal(t, smallCount, readSmallCount)
	require.Equal(t, largeCount, readLargeCount)
}

func TestTopicMultiWriter_AutoPartitioning_OrderPerProducerAfterSplit(t *testing.T) {
	scope := newScope(t)
	runTestWithAutoPartitioning(t, scope)
}

func TestTopicMultiWriter_BoundChooser_FirstPartitionKeyRemainsStableAfterSplit(t *testing.T) {
	scope := newScope(t)
	scenario, describe := newAutoPartitioningScenario(t, scope, "--auto-part-first-key--", "autopartitioning_first_key")
	payload := bytes.Repeat([]byte{'b'}, 1<<20)

	writeLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range scenario.getKeys(partitions) {
			scenario.writeMessages(scenario.writer1, payload, key, nil, 1)
			scenario.writeMessages(scenario.writer2, payload, key, nil, 1)
		}
		scenario.flushBoth(scenario.ctx)
	}

	writeLoadRound(describe.Partitions)
	writeLoadRound(describe.Partitions)
	describe = scenario.waitForPartitionsCountAtLeast(4, 20*time.Second, writeLoadRound)
	require.GreaterOrEqual(t, len(describe.Partitions), 4)

	specialPayloads := [][]byte{
		[]byte("first-partition-key-after-split-1"),
		[]byte("first-partition-key-after-split-2"),
		[]byte("first-partition-key-after-split-3"),
	}
	for _, payload := range specialPayloads {
		scenario.writeMessages(scenario.writer1, payload, scenario.firstPartitionKey, nil, 1)
	}
	scenario.flushBoth(scenario.ctx)
	scenario.closeAll(scope.Ctx)

	partitionsByPayload, err := readPartitionByPayload(
		scope.Ctx,
		scenario.topicClient,
		scenario.topicPath,
		specialPayloads,
		defaultReadTimeout,
	)
	require.NoError(t, err)

	var expectedPartition int64
	for i, payload := range specialPayloads {
		partitionID := partitionsByPayload[string(payload)]
		if i == 0 {
			expectedPartition = partitionID

			continue
		}

		require.Equal(t, expectedPartition, partitionID)
	}
}

func TestTopicMultiWriter_AutoPartitioning_NoPayloadCorruptionOnResend(t *testing.T) {
	scope := newScope(t)
	scenario, describe := newAutoPartitioningScenario(t, scope, "--auto-part-no-corruption--", "autopartitioning_payload")

	expectedPayloads := make(map[string]struct{})
	uniquePayload := func(prefix string, idx int) []byte {
		payload := bytes.Repeat([]byte{'p'}, 1<<20)
		copy(payload, []byte(fmt.Sprintf("%s-%03d", prefix, idx)))
		expectedPayloads[string(payload)] = struct{}{}

		return payload
	}

	messageIdx := 0
	writeUniqueLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range scenario.getKeys(partitions) {
			scenario.writeMessages(scenario.writer1, uniquePayload("w1", messageIdx), key, nil, 1)
			messageIdx++
			scenario.writeMessages(scenario.writer2, uniquePayload("w2", messageIdx), key, nil, 1)
			messageIdx++
		}
		scenario.flushBoth(scenario.ctx)
	}

	writeUniqueLoadRound(describe.Partitions)
	writeUniqueLoadRound(describe.Partitions)
	describe = scenario.waitForPartitionsCountAtLeast(4, 20*time.Second, writeUniqueLoadRound)
	writeUniqueLoadRound(describe.Partitions)
	scenario.closeAll(scope.Ctx)

	messages, err := readMessagesDetailed(
		scope.Ctx,
		scenario.topicClient,
		scenario.topicPath,
		consumerName,
		len(expectedPayloads),
		2*time.Minute,
	)
	require.NoError(t, err)
	require.NoError(t, assertMessagesOrderedBySeqNo(messages))

	seenPayloads := make(map[string]struct{}, len(messages))
	for _, msg := range messages {
		payloadKey := string(msg.payload)
		if _, ok := expectedPayloads[payloadKey]; !ok {
			t.Fatalf("unexpected payload read back, size=%d", len(msg.payload))
		}
		seenPayloads[payloadKey] = struct{}{}
	}
	require.Len(t, seenPayloads, len(expectedPayloads))
}

func TestTopicMultiWriter_AutoPartitioning_MetadataPreservedOnResend(t *testing.T) {
	scope := newScope(t)
	scenario, describe := newAutoPartitioningScenario(t, scope, "--auto-part-metadata--", "autopartitioning_metadata")

	expectedMetadata := make(map[string]string)
	payloadFor := func(prefix string, idx int) []byte {
		payload := bytes.Repeat([]byte{'m'}, 1<<20)
		copy(payload, []byte(fmt.Sprintf("%s-payload-%03d", prefix, idx)))

		return payload
	}

	messageIdx := 0
	writeMetadataLoadRound := func(partitions []topictypes.PartitionInfo) {
		for _, key := range scenario.getKeys(partitions) {
			payload1 := payloadFor("w1", messageIdx)
			meta1 := fmt.Sprintf("meta-w1-%03d", messageIdx)
			expectedMetadata[string(payload1)] = meta1
			scenario.writeMessages(scenario.writer1, payload1, key, map[string][]byte{
				"custom-meta": []byte(meta1),
			}, 1)
			messageIdx++

			payload2 := payloadFor("w2", messageIdx)
			meta2 := fmt.Sprintf("meta-w2-%03d", messageIdx)
			expectedMetadata[string(payload2)] = meta2
			scenario.writeMessages(scenario.writer2, payload2, key, map[string][]byte{
				"custom-meta": []byte(meta2),
			}, 1)
			messageIdx++
		}
		scenario.flushBoth(scenario.ctx)
	}

	writeMetadataLoadRound(describe.Partitions)
	writeMetadataLoadRound(describe.Partitions)
	_ = scenario.waitForPartitionsCountAtLeast(4, 20*time.Second, writeMetadataLoadRound)
	scenario.closeAll(scope.Ctx)

	messages, err := readMessagesDetailed(
		scope.Ctx,
		scenario.topicClient,
		scenario.topicPath,
		consumerName,
		len(expectedMetadata),
		2*time.Minute,
	)
	require.NoError(t, err)
	require.NoError(t, assertMessagesOrderedBySeqNo(messages))

	for _, msg := range messages {
		expectedMeta, ok := expectedMetadata[string(msg.payload)]
		require.True(t, ok, "unexpected payload %q", string(msg.payload))
		require.Equal(t, expectedMeta, string(msg.metadata["custom-meta"]))
	}
}

func TestTopicMultiWriter_AutoPartitioning_SplitDuringInFlightBatch(t *testing.T) {
	scope := newScope(t)
	scenario, describe := newAutoPartitioningScenario(t, scope, "--auto-part-inflight--", "autopartitioning_inflight")
	payload := bytes.Repeat([]byte{'i'}, 1<<20)
	var written1, written2 int

	writeBurstNoFlush := func(partitions []topictypes.PartitionInfo) {
		for _, key := range scenario.getKeys(partitions) {
			for range 3 {
				written1 += scenario.writeMessages(scenario.writer1, payload, key, nil, 1)
				written2 += scenario.writeMessages(scenario.writer2, payload, key, nil, 1)
			}
		}
	}

	writeBurstNoFlush(describe.Partitions)
	writeBurstNoFlush(describe.Partitions)
	_ = scenario.waitForPartitionsCountAtLeast(3, 20*time.Second, writeBurstNoFlush)

	flushCtx, cancel := context.WithTimeout(scope.Ctx, 45*time.Second)
	defer cancel()
	require.NoError(t, scenario.writer1.Flush(flushCtx))
	require.NoError(t, scenario.writer2.Flush(flushCtx))
	scenario.closeAll(scope.Ctx)

	require.NoError(t, readMessagesAndAssertOrderedBySeqNo(
		scope.Ctx,
		scenario.topicClient,
		scenario.topicPath,
		consumerName,
		written1+written2,
		2*time.Minute,
		payload,
	))
}

func TestTopicMultiWriter_AutoPartitioning_SmallMessages(t *testing.T) {
	const (
		smallMessageSize      = 65536 // 64KB
		logicalWriteSizeBytes = 1 << 20
	)

	scope := newScope(t)
	xtest.TestManyTimes(
		t,
		func(t testing.TB) {
			runTestWithAutoPartitioningVariant(t, scope, autoPartitioningIntegrationVariant{
				topicPathMarker:         "--auto-part-topic-small--",
				payload:                 bytes.Repeat([]byte{'s'}, smallMessageSize),
				producerStem:            "autopartitioning_small",
				messagesPerLogicalWrite: logicalWriteSizeBytes / smallMessageSize,
				readTimeout:             3 * time.Minute,
			})
		},
		xtest.StopAfter(60*time.Second),
	)
}
