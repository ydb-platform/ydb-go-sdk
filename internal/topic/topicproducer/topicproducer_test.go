package topicproducer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicproducer/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type stubWritersFactory struct {
	stubWriterType   stubs.StubWriterType
	producerIDPrefix string
	describeSplits   *stubs.DescribeWithSplitsState
	seqNoMap         map[string]int64
	mu               xsync.Mutex
}

func newStubWritersFactory(
	stubWriterType stubs.StubWriterType,
	producerIDPrefix string,
	describeSplits *stubs.DescribeWithSplitsState,
) *stubWritersFactory {
	return &stubWritersFactory{
		stubWriterType:   stubWriterType,
		producerIDPrefix: producerIDPrefix,
		describeSplits:   describeSplits,
		seqNoMap:         make(map[string]int64),
	}
}

func (f *stubWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	switch f.stubWriterType {
	case stubs.StubWriterTypeBasic:
		return stubs.NewBasicWriter(cfg.OnAckReceivedCallback, cfg.AutoSetSeqNo), nil
	case stubs.StubWriterTypeWithAutopartitioning:
		partitionID, _ := cfg.PartitionID()
		producerID := cfg.ProducerID()
		var onSplit func(int64)
		if f.describeSplits != nil {
			onSplit = f.describeSplits.RecordSplit
		}
		updateSeqNo := func(producerID string, seqNo int64) {
			f.mu.WithLock(func() {
				f.seqNoMap[producerID] = max(f.seqNoMap[producerID], seqNo)
			})
		}

		var maxSeqNo int64
		f.mu.WithLock(func() {
			maxSeqNo = f.seqNoMap[producerID]
		})

		return stubs.NewWriterWithAutopartitioning(
			cfg.OnAckReceivedCallback,
			cfg.RetrySettings,
			cfg.AutoSetSeqNo,
			partitionID,
			onSplit,
			updateSeqNo,
			maxSeqNo,
			f.producerIDPrefix,
		), nil
	default:
		return nil, errors.New("invalid stub writer type")
	}
}

func newTestProducer(t testing.TB, describer TopicDescriber) *Producer {
	t.Helper()

	return NewProducer(describer, ProducerConfig{})
}

// newTestProducerWithBasicWriter creates a producer that uses basicWriter as writer (no real gRPC).
func newTestProducerWithBasicWriter(t testing.TB, describer TopicDescriber) *Producer {
	t.Helper()
	cfg := ProducerConfig{}
	withWritersFactory(newStubWritersFactory(stubs.StubWriterTypeBasic, "test-producer", nil))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithBasicWriterOptions(
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithMaxQueueLen(100),
		topicwriterinternal.WithAutosetCreatedTime(false),
	)(&cfg)

	return NewProducer(describer, cfg)
}

// newTestProducerWithAutopartitioningWriter creates a producer that uses the autopartitioning stub:
// client must be from NewStubTopicClientWithSplits(t, state). After 100 messages the writer returns
// OVERLOADED, main_worker calls onPartitionSplit and Describe; state adds two child partitions
// with bounds [from, (from+to)/2) and [(from+to)/2, to).
func newTestProducerWithAutopartitioningWriter(
	t testing.TB,
	describer TopicDescriber,
	state *stubs.DescribeWithSplitsState,
) *Producer {
	const producerIDPrefix = "test-producer"

	t.Helper()
	cfg := ProducerConfig{}
	withWritersFactory(newStubWritersFactory(stubs.StubWriterTypeWithAutopartitioning, producerIDPrefix, state))(&cfg)
	WithProducerIDPrefix(producerIDPrefix)(&cfg)
	WithBasicWriterOptions(
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithMaxQueueLen(100),
		topicwriterinternal.WithAutosetCreatedTime(false),
	)(&cfg)

	return NewProducer(describer, cfg)
}

func TestProducer_ErrAlreadyClosed(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducer(t, func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubClient.Describe(ctx, path)
	})

	err := producer.Close(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestProducer_WaitInit_Success(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducer(t, func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubClient.Describe(ctx, path)
	})

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.NoError(t, err)
}

func TestProducer_WaitInit_ContextCanceled(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducer(t, func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubClient.Describe(ctx, path)
	})

	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()

	err := producer.WaitInit(ctxCancel)
	require.ErrorIs(t, err, context.Canceled)

	_ = producer.Close(ctx)
}

func TestProducer_DescribeError(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	describeErr := errors.New("describe failed")
	stubClient := stubs.NewStubTopicClientWithError(t, describeErr)
	producer := newTestProducer(t, func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubClient.Describe(ctx, path)
	})

	ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := producer.WaitInit(ctxTimeout)
	require.Error(t, err)
	// When init fails, WaitInit may return context.DeadlineExceeded (initDone is not closed on error)
	// or the error may propagate depending on timing
	require.True(t, errors.Is(err, describeErr) || errors.Is(err, context.DeadlineExceeded))
}

func TestProducer_Write_WithBasicWriter(t *testing.T) {
	t.Parallel()

	var (
		ctx           = xtest.Context(t)
		sentTimestamp = make(map[int64]time.Time)
		mu            = xsync.Mutex{}
	)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]Message, 0, 1000)
	for i := range 1000 {
		mu.WithLock(func() {
			sentTimestamp[int64(i+1)] = time.Now()
		})
		messages = append(messages, Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 1),
			},
			Key: fmt.Sprintf("partition-key-%d", i),
		})
	}

	err = producer.Write(ctx, messages...)
	require.NoError(t, err)

	err = producer.Flush(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.NoError(t, err)
}

func TestProducer_Write_WithAutopartitioningWriter(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	baseDesc := stubs.DefaultStubTopicDescription()
	state := stubs.NewDescribeWithSplitsState(baseDesc, 6) // 6 is first free ID after partitions 1..5
	stubClient := stubs.NewStubTopicClientWithSplits(t, state)
	producer := newTestProducerWithAutopartitioningWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		state,
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	// Send enough messages so at least one partition hits MessagesBeforeOverloaded and returns OVERLOADED.
	// onPartitionSplit uses writersFactory.Create + writer.WaitInit (same stub), so split path runs without StartWriter.
	messages := make([]Message, 0, 1000)
	for i := range 1000 {
		messages = append(messages, Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 1),
			},
			Key: fmt.Sprintf("partition-key-%d", i+1),
		})
	}

	err = producer.Write(ctx, messages...)
	require.NoError(t, err)

	err = producer.Flush(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.NoError(t, err)
}
