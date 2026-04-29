package topicmultiwriter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var errTest = errors.New("test error")

func (p *MultiWriter) getWritersCount() int {
	return p.orchestrator.getWritersCount()
}

// for test purposes
func (o *orchestrator) getWritersCount() int {
	return o.writerPool.getWritersCount()
}

type stubWritersFactory struct {
	t                testing.TB
	stubWriterType   stubs.StubWriterType
	producerIDPrefix string
	describeSplits   *stubs.DescribeWithSplitsState
	seqNoMap         map[string]int64
	mu               xsync.Mutex
	ackDelay         time.Duration
}

func newStubWritersFactory(
	t testing.TB,
	stubWriterType stubs.StubWriterType,
	producerIDPrefix string,
	describeSplits *stubs.DescribeWithSplitsState,
	ackDelay time.Duration,
) *stubWritersFactory {
	return &stubWritersFactory{
		t:                t,
		stubWriterType:   stubWriterType,
		producerIDPrefix: producerIDPrefix,
		describeSplits:   describeSplits,
		seqNoMap:         make(map[string]int64),
		ackDelay:         ackDelay,
	}
}

func (f *stubWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	switch f.stubWriterType {
	case stubs.StubWriterTypeBasic:
		return stubs.NewBasicWriter(f.t, cfg.OnAckReceivedCallback, cfg.AutoSetSeqNo, f.ackDelay), nil
	case stubs.StubWriterTypeError:
		return nil, errTest
	case stubs.StubWriterTypeWithAutopartitioning:
		partitionID, _ := cfg.PartitionID()
		producerID := cfg.ProducerID()

		var onSplit func(int64)
		if f.describeSplits != nil && f.describeSplits.IsBasePartition(partitionID) {
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
			f.t,
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

type choosePartitionKeyCheckWritersFactory struct {
	t testing.TB
}

func (f *choosePartitionKeyCheckWritersFactory) Create(
	cfg topicwriterinternal.WriterReconnectorConfig,
) (writer, error) {
	return stubs.NewBasicWriter(
		f.t,
		cfg.OnAckReceivedCallback,
		cfg.AutoSetSeqNo,
		0,
		stubs.WithRequireChoosePartitionKeyMetadata(f.t),
	), nil
}

func newTestMultiWriter(t testing.TB, describer TopicDescriber) *MultiWriter {
	t.Helper()

	cfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "test-producer", nil, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)

	writer, err := NewMultiWriter(describer, &topicwriterinternal.WriterReconnectorConfig{}, &cfg)
	require.NoError(t, err)

	return writer
}

func newTestMultiWriterWithInitDelay(
	t testing.TB,
	describer TopicDescriber,
	initDelay time.Duration,
) *MultiWriter {
	t.Helper()
	cfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "test-producer", nil, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)

	writer, err := NewMultiWriter(func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		time.Sleep(initDelay)

		return describer(ctx, path)
	}, &topicwriterinternal.WriterReconnectorConfig{}, &cfg)
	require.NoError(t, err)

	return writer
}

// newTestMultiWriterWithBasicWriter creates a multiwriter that uses basicWriter as writer (no real gRPC).
func newTestMultiWriterWithBasicWriter(
	t testing.TB,
	describer TopicDescriber,
	opts ...topicwriterinternal.PublicWriterOption,
) *MultiWriter {
	t.Helper()
	cfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "test-producer", nil, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)
	for _, opt := range opts {
		opt(writerCfg)
	}

	writer, err := NewMultiWriter(describer, writerCfg, &cfg)
	require.NoError(t, err)

	return writer
}

// newTestMultiWriterWithAutopartitioningWriter creates a multiwriter that uses the autopartitioning stub:
// client must be from NewStubTopicClientWithSplits(t, state). After 100 messages the writer returns
// OVERLOADED, orchestrator calls onPartitionSplit and Describe; state adds two child partitions
// with bounds [from, (from+to)/2) and [(from+to)/2, to).
func newTestMultiWriterWithAutopartitioningWriter(
	t testing.TB,
	describer TopicDescriber,
	state *stubs.DescribeWithSplitsState,
) *MultiWriter {
	const producerIDPrefix = "test-producer"

	t.Helper()
	cfg := MultiWriterConfig{}
	withWritersFactory(
		newStubWritersFactory(
			t,
			stubs.StubWriterTypeWithAutopartitioning,
			producerIDPrefix,
			state,
			0,
		),
	)(&cfg)
	WithProducerIDPrefix(producerIDPrefix)(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)
	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)

	writer, err := NewMultiWriter(describer, writerCfg, &cfg)
	require.NoError(t, err)

	return writer
}

func newTestMultiWriterWithSmallIdleSessionTimeout(t testing.TB, describer TopicDescriber) *MultiWriter {
	t.Helper()
	cfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "test-producer", nil, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterIdleTimeout(1 * time.Second)(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)

	writer, err := NewMultiWriter(describer, writerCfg, &cfg)
	require.NoError(t, err)

	return writer
}

func newTestMultiWriterWithAckDelay(
	t testing.TB,
	describer TopicDescriber,
	ackDelay time.Duration,
	opts ...topicwriterinternal.PublicWriterOption,
) *MultiWriter {
	t.Helper()
	cfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "test-producer", nil, ackDelay))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)

	for _, opt := range opts {
		opt(writerCfg)
	}

	writer, err := NewMultiWriter(describer, writerCfg, &cfg)
	require.NoError(t, err)

	return writer
}

func newTestMultiWriterWithCustomWritersFactory(
	t testing.TB,
	describer TopicDescriber,
	writersFactory writersFactory,
	opts ...topicwriterinternal.PublicWriterOption,
) *MultiWriter {
	t.Helper()
	cfg := MultiWriterConfig{}
	withWritersFactory(writersFactory)(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(&cfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)

	for _, opt := range opts {
		opt(writerCfg)
	}

	writer, err := NewMultiWriter(describer, writerCfg, &cfg)
	require.NoError(t, err)

	return writer
}

func TestMultiWriter_ErrAlreadyClosed(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriter(t, func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubClient.Describe(ctx, path)
	})

	err := multiWriter.Close(ctx)
	require.NoError(t, err)

	err = multiWriter.Close(ctx)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestMultiWriter_WaitInit_Success(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriter(t, func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubClient.Describe(ctx, path)
	})

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	err = multiWriter.Close(ctx)
	require.NoError(t, err)
}

func TestMultiWriter_WaitInit_ContextCanceled(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithInitDelay(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		time.Second*10,
	)

	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()

	err := multiWriter.WaitInit(ctxCancel)
	require.ErrorIs(t, err, context.Canceled)

	_ = multiWriter.Close(ctxCancel)
}

func TestProducer_CloseWithoutWaitInit(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()

	err := multiWriter.Close(ctxTimeout)
	require.NoError(t, err)
}

func TestMultiWriter_DescribeError(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	describeErr := errors.New("describe failed")
	stubClient := stubs.NewStubTopicClientWithError(t, describeErr)
	multiWriter := newTestMultiWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	ctxTimeout, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	err := multiWriter.WaitInit(ctxTimeout)
	require.Error(t, err)
	// When init fails, WaitInit may return context.DeadlineExceeded (initDone is not closed on error)
	// or the error may propagate depending on timing
	require.True(t, errors.Is(err, describeErr) || errors.Is(err, context.DeadlineExceeded))
}

func TestMultiWriter_Write_WithBasicWriter(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]topicwriterinternal.PublicMessage, 0, 1000)
	for i := range 1000 {
		messages = append(messages, topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: int64(i + 1),
			Key:   fmt.Sprintf("partition-key-%d", i+1),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages))
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_Write_SetsChoosePartitionKeyMetadata(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		&choosePartitionKeyCheckWritersFactory{t: t},
	)

	require.NoError(t, multiWriter.WaitInit(ctx))

	messages := []topicwriterinternal.PublicMessage{{
		Data:     bytes.NewReader([]byte("hello")),
		SeqNo:    1,
		Key:      "partition-key",
		Metadata: map[string][]byte{},
	}}

	require.NoError(t, multiWriter.Write(ctx, messages))
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_Write_WaitForAck(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		topicwriterinternal.WithWaitAckOnWrite(true),
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]topicwriterinternal.PublicMessage, 0, 1000)
	for i := range 1000 {
		messages = append(messages, topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: int64(i + 1),
			Key:   fmt.Sprintf("partition-key-%d", i),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages))
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_Write_WithErrorWritersFactory(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		newStubWritersFactory(t, stubs.StubWriterTypeError, "test-producer", nil, 0),
	)

	err := multiWriter.WaitInit(ctx)
	require.ErrorIs(t, err, errTest)
}

func TestMultiWriter_Write_MaxQueueLenExceeded(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithAckDelay(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		time.Second*2,
		topicwriterinternal.WithMaxQueueLen(5),
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	messages := make([]topicwriterinternal.PublicMessage, 0, 5)
	for i := range 5 {
		messages = append(messages, topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: int64(i + 1),
			Key:   fmt.Sprintf("partition-key-%d", i+1),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages))

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	require.ErrorIs(t, multiWriter.Write(ctxTimeout, []topicwriterinternal.PublicMessage{
		{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: 6,
			Key:   fmt.Sprintf("partition-key-%d", 6),
		},
	}), context.DeadlineExceeded)
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_Write_CloseTimeout(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithAckDelay(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		time.Second*2,
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: 1,
			Key:   "partition-key-1",
		},
	}))

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	require.ErrorIs(t, multiWriter.Close(ctxTimeout), context.DeadlineExceeded)
}

func TestMultiWriter_Write_ErrNoSeqNo(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	err = multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{
			Data: bytes.NewReader([]byte("hello")),
			Key:  "partition-key-1",
		},
	})

	require.ErrorIs(t, err, ErrNoSeqNo)
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_WaitInitInfo_Unimplemented(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	_, err := multiWriter.WaitInitInfo(ctx)
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestMultiWriter_CloseOfClosed(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	require.NoError(t, multiWriter.Close(ctx))
	require.ErrorIs(t, multiWriter.Close(ctx), ErrAlreadyClosed)
}

func TestMultiWriter_Write_Parallel(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		topicwriterinternal.WithAutoSetSeqNo(true),
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	var errgroup errgroup.Group
	errgroup.SetLimit(10)

	for i := range 1000 {
		errgroup.Go(func() error {
			return multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
				{
					Data: bytes.NewReader([]byte("hello")),
					Key:  fmt.Sprintf("partition-key-%d", i+1),
				},
			})
		})
	}

	require.NoError(t, errgroup.Wait())
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_Write_WithAutopartitioningWriter(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	baseDesc := stubs.DefaultStubTopicDescription(t)
	state := stubs.NewDescribeWithSplitsState(t, baseDesc, 6) // 6 is first free ID after partitions 1..5
	stubClient := stubs.NewStubTopicClientWithSplits(t, state)
	multiWriter := newTestMultiWriterWithAutopartitioningWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		state,
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	// Send enough messages so at least one partition hits MessagesBeforeOverloaded and returns OVERLOADED.
	// onPartitionSplit uses writersFactory.Create + writer.WaitInit (same stub), so split path runs without StartWriter.
	messages := make([]topicwriterinternal.PublicMessage, 0, 1000)
	for i := range 1000 {
		messages = append(messages, topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: int64(i + 1),
			Key:   fmt.Sprintf("partition-key-%d", i+1),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages))
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_Write_SmallIdleSessionTimeout(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	baseDesc := stubs.DefaultStubTopicDescription(t)
	state := stubs.NewDescribeWithSplitsState(t, baseDesc, 6) // 6 is first free ID after partitions 1..5
	stubClient := stubs.NewStubTopicClientWithSplits(t, state)
	multiWriter := newTestMultiWriterWithSmallIdleSessionTimeout(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	err := multiWriter.WaitInit(ctx)
	require.NoError(t, err)

	// Send enough messages so at least one partition hits MessagesBeforeOverloaded and returns OVERLOADED.
	// onPartitionSplit uses writersFactory.Create + writer.WaitInit (same stub), so split path runs without StartWriter.
	messages := make([]topicwriterinternal.PublicMessage, 0, 5)
	for i := range 5 {
		messages = append(messages, topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: int64(i + 1),
			Key:   fmt.Sprintf("partition-key-%d", i+1),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages))
	require.NoError(t, multiWriter.Flush(ctx))

	time.Sleep(2 * time.Second)
	require.Equal(t, 0, multiWriter.getWritersCount())
	messages = messages[:0]
	for i := range 5 {
		messages = append(messages, topicwriterinternal.PublicMessage{
			Data:  bytes.NewReader([]byte("hello")),
			SeqNo: int64(i + 6),
			Key:   fmt.Sprintf("partition-key-%d", i+6),
		})
	}

	require.NoError(t, multiWriter.Write(ctx, messages))
	require.NoError(t, multiWriter.Close(ctx))
}
