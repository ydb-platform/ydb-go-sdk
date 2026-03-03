package topicproducer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicproducer/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var errTest = errors.New("test error")

type stubWritersFactory struct {
	stubWriterType   stubs.StubWriterType
	producerIDPrefix string
	describeSplits   *stubs.DescribeWithSplitsState
	seqNoMap         map[string]int64
	mu               xsync.Mutex
	ackDelay         time.Duration
	initDelay        time.Duration
}

func newStubWritersFactory(
	stubWriterType stubs.StubWriterType,
	producerIDPrefix string,
	describeSplits *stubs.DescribeWithSplitsState,
	ackDelay time.Duration,
	initDelay time.Duration,
) *stubWritersFactory {
	return &stubWritersFactory{
		stubWriterType:   stubWriterType,
		producerIDPrefix: producerIDPrefix,
		describeSplits:   describeSplits,
		seqNoMap:         make(map[string]int64),
		ackDelay:         ackDelay,
		initDelay:        initDelay,
	}
}

func (f *stubWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	switch f.stubWriterType {
	case stubs.StubWriterTypeBasic:
		return stubs.NewBasicWriter(cfg.OnAckReceivedCallback, cfg.AutoSetSeqNo, f.ackDelay, f.initDelay), nil
	case stubs.StubWriterTypeError:
		return nil, errTest
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

func newTestProducerWithInitDelay(
	t testing.TB,
	describer TopicDescriber,
	initDelay time.Duration,
) *Producer {
	t.Helper()
	cfg := ProducerConfig{}
	withWritersFactory(newStubWritersFactory(stubs.StubWriterTypeBasic, "test-producer", nil, 0, initDelay))(&cfg)

	return NewProducer(describer, cfg)
}

// newTestProducerWithBasicWriter creates a producer that uses basicWriter as writer (no real gRPC).
func newTestProducerWithBasicWriter(
	t testing.TB,
	describer TopicDescriber,
	opts ...topicwriterinternal.PublicWriterOption,
) *Producer {
	t.Helper()
	cfg := ProducerConfig{}
	withWritersFactory(newStubWritersFactory(stubs.StubWriterTypeBasic, "test-producer", nil, 0, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)

	options := []topicwriterinternal.PublicWriterOption{
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithMaxQueueLen(100),
		topicwriterinternal.WithAutosetCreatedTime(false),
	}
	options = append(options, opts...)
	WithBasicWriterOptions(options...)(&cfg)

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
	withWritersFactory(newStubWritersFactory(stubs.StubWriterTypeWithAutopartitioning, producerIDPrefix, state, 0, 0))(&cfg)
	WithProducerIDPrefix(producerIDPrefix)(&cfg)
	WithBasicWriterOptions(
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithMaxQueueLen(100),
		topicwriterinternal.WithAutosetCreatedTime(false),
	)(&cfg)

	return NewProducer(describer, cfg)
}

func newTestProducerWithSmallIdleSessionTimeout(t testing.TB, describer TopicDescriber) *Producer {
	t.Helper()
	cfg := ProducerConfig{}
	withWritersFactory(newStubWritersFactory(stubs.StubWriterTypeBasic, "test-producer", nil, 0, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithSubSessionIdleTimeout(1 * time.Second)(&cfg)
	WithBasicWriterOptions(
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithMaxQueueLen(100),
		topicwriterinternal.WithAutosetCreatedTime(false),
	)(&cfg)

	return NewProducer(describer, cfg)
}

func newTestProducerWithAckDelay(
	t testing.TB,
	describer TopicDescriber,
	ackDelay time.Duration,
	opts ...topicwriterinternal.PublicWriterOption,
) *Producer {
	t.Helper()
	cfg := ProducerConfig{}
	withWritersFactory(newStubWritersFactory(stubs.StubWriterTypeBasic, "test-producer", nil, ackDelay, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)

	options := []topicwriterinternal.PublicWriterOption{
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithAutosetCreatedTime(false),
	}
	options = append(options, opts...)
	WithBasicWriterOptions(options...)(&cfg)

	return NewProducer(describer, cfg)
}

func newTestProducerWithCustomWritersFactory(
	t testing.TB,
	describer TopicDescriber,
	writersFactory writersFactory,
	opts ...topicwriterinternal.PublicWriterOption,
) *Producer {
	t.Helper()
	cfg := ProducerConfig{}
	withWritersFactory(writersFactory)(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)

	options := []topicwriterinternal.PublicWriterOption{
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithAutosetCreatedTime(false),
	}
	options = append(options, opts...)
	WithBasicWriterOptions(options...)(&cfg)

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
	producer := newTestProducerWithInitDelay(t, func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubClient.Describe(ctx, path)
	}, time.Second*10)

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

	ctx := xtest.Context(t)

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
		messages = append(messages, Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 1),
			},
			Key: fmt.Sprintf("partition-key-%d", i),
		})
	}

	require.NoError(t, producer.Write(ctx, messages...))
	require.NoError(t, producer.Close(ctx))
}

func TestProducer_Write_WaitForAck(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		topicwriterinternal.WithWaitAckOnWrite(true),
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]Message, 0, 1000)
	for i := range 1000 {
		messages = append(messages, Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 1),
			},
			Key: fmt.Sprintf("partition-key-%d", i),
		})
	}

	require.NoError(t, producer.Write(ctx, messages...))
	require.NoError(t, producer.Close(ctx))
}

func TestProducer_Write_WithErrorWritersFactory(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithCustomWritersFactory(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		newStubWritersFactory(stubs.StubWriterTypeError, "test-producer", nil, 0, 0),
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	err = producer.Write(ctx, Message{
		PublicMessage: topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: 1,
		},
		Key: "partition-key-1",
	})
	require.NoError(t, err)
	require.ErrorIs(t, producer.Flush(ctx), errTest)
	require.ErrorIs(t, producer.Close(ctx), errTest)
}

func TestProducer_Write_MaxQueueLenExceeded(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithAckDelay(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		time.Second*2,
		topicwriterinternal.WithMaxQueueLen(10),
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]Message, 0, 5)
	for i := range 5 {
		messages = append(messages, Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 1),
			},
			Key: fmt.Sprintf("partition-key-%d", i),
		})
	}

	require.NoError(t, producer.Write(ctx, messages...))

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	require.ErrorIs(t, producer.Write(ctxTimeout, Message{
		PublicMessage: topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: 6,
		},
		Key: "partition-key-6",
	}), context.DeadlineExceeded)
	require.NoError(t, producer.Close(ctx))
}

func TestProducer_Write_CloseTimeout(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithAckDelay(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		time.Second*2,
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	require.NoError(t, producer.Write(ctx, Message{
		PublicMessage: topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: 1,
		},
		Key: "partition-key-1",
	}))

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	require.ErrorIs(t, producer.Close(ctxTimeout), context.DeadlineExceeded)
}

func TestProducer_Write_ErrNoSeqNo(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	err = producer.Write(ctx, Message{
		PublicMessage: topicwriterinternal.PublicMessage{
			Data: bytes.NewReader([]byte("hello")),
		},
		Key: fmt.Sprintf("partition-key-%d", 1),
	})

	require.ErrorIs(t, err, ErrNoSeqNo)
	require.NoError(t, producer.Close(ctx))
}

func TestProducer_CloseOfClosed(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	require.NoError(t, producer.Close(ctx))
	require.ErrorIs(t, producer.Close(ctx), ErrAlreadyClosed)
}

func TestProducer_Write_Parallel(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducerWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		topicwriterinternal.WithAutoSetSeqNo(true),
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	var errgroup errgroup.Group
	errgroup.SetLimit(10)

	for i := range 1000 {
		errgroup.Go(func() error {
			return producer.Write(ctx, Message{
				PublicMessage: topicwriterinternal.PublicMessage{
					Data: bytes.NewReader([]byte("hello")),
				},
				Key: fmt.Sprintf("partition-key-%d", i+1),
			})
		})
	}

	require.NoError(t, errgroup.Wait())
	require.NoError(t, producer.Close(ctx))
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

	require.NoError(t, producer.Write(ctx, messages...))
	require.NoError(t, producer.Close(ctx))
}

func TestProducer_Write_SmallIdleSessionTimeout(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	baseDesc := stubs.DefaultStubTopicDescription()
	state := stubs.NewDescribeWithSplitsState(baseDesc, 6) // 6 is first free ID after partitions 1..5
	stubClient := stubs.NewStubTopicClientWithSplits(t, state)
	producer := newTestProducerWithSmallIdleSessionTimeout(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	// Send enough messages so at least one partition hits MessagesBeforeOverloaded and returns OVERLOADED.
	// onPartitionSplit uses writersFactory.Create + writer.WaitInit (same stub), so split path runs without StartWriter.
	messages := make([]Message, 0, 5)
	for i := range 5 {
		messages = append(messages, Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 1),
			},
			Key: fmt.Sprintf("partition-key-%d", i+1),
		})
	}

	require.NoError(t, producer.Write(ctx, messages...))
	require.NoError(t, producer.Flush(ctx))

	time.Sleep(2 * time.Second)
	require.Equal(t, 0, producer.getWritersCount())
	messages = messages[:0]
	for i := range 5 {
		messages = append(messages, Message{
			PublicMessage: topicwriterinternal.PublicMessage{
				Data:  bytes.NewReader([]byte("hello")),
				SeqNo: int64(i + 6),
			},
			Key: fmt.Sprintf("partition-key-%d", i+6),
		})
	}

	require.NoError(t, producer.Write(ctx, messages...))
	require.NoError(t, producer.Close(ctx))
}
