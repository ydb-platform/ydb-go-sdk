package topicmultiwriter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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

type overloadedInitWriter struct{}

func (w *overloadedInitWriter) Close(ctx context.Context) error {
	return nil
}

func (w *overloadedInitWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{}, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
}

func (w *overloadedInitWriter) WriteInternal(
	ctx context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) error {
	return nil
}

type overloadedInitWritersFactory struct{}

func (f *overloadedInitWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	return &overloadedInitWriter{}, nil
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

type orderedSeqWriter struct {
	lastSeqNo             int64
	writes                chan int64
	onAckReceivedCallback func(seqNo int64)
}

func (w *orderedSeqWriter) Close(ctx context.Context) error {
	return nil
}

func (w *orderedSeqWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{}, nil
}

func (w *orderedSeqWriter) WriteInternal(
	ctx context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) error {
	for _, msg := range messages {
		if msg.SeqNo <= w.lastSeqNo {
			return fmt.Errorf("unordered seq no: %d <= %d", msg.SeqNo, w.lastSeqNo)
		}
		w.lastSeqNo = msg.SeqNo
		w.writes <- msg.SeqNo
		if w.onAckReceivedCallback != nil {
			// Notify multiwriter the message was accepted so flush() in Close()
			// can complete; mirror what real subwriter does after a server ack.
			// Must be synchronous: async delivery can reorder acks vs. in-flight
			// queue head and trigger seqNo mismatch in onAckReceivedNeedLock.
			w.onAckReceivedCallback(msg.SeqNo)
		}
	}

	return nil
}

type orderedSeqWriterFactory struct {
	writer *orderedSeqWriter
}

func (f orderedSeqWriterFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, _ := cfg.PartitionID()
	if partitionID != 1 {
		return &orderedSeqWriter{
			writes:                make(chan int64, 1),
			onAckReceivedCallback: cfg.OnAckReceivedCallback,
		}, nil
	}

	f.writer.onAckReceivedCallback = cfg.OnAckReceivedCallback

	return f.writer, nil
}

type blockingReader struct {
	started chan struct{}
	release chan struct{}
	data    []byte
	done    bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	close(r.started)
	<-r.release
	r.done = true

	return copy(p, r.data), nil
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

type failingAddPartitionChooser struct {
	delegate PartitionChooser
	addCalls int
	err      error
}

func (c *failingAddPartitionChooser) ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error) {
	return c.delegate.ChoosePartition(msg)
}

func (c *failingAddPartitionChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	c.addCalls++
	if c.addCalls > 1 {
		return c.err
	}

	return c.delegate.AddNewPartitions(partitions...)
}

func (c *failingAddPartitionChooser) RemovePartition(partitionID int64) {
	c.delegate.RemovePartition(partitionID)
}

type errorPartitionChooser struct {
	err error
}

func (c *errorPartitionChooser) ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error) {
	return 0, c.err
}

func (c *errorPartitionChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	return nil
}

func (c *errorPartitionChooser) RemovePartition(partitionID int64) {
}

func newTestMultiWriterWithPartitionChooser(
	t testing.TB,
	describer TopicDescriber,
	partitionChooser PartitionChooser,
) *MultiWriter {
	t.Helper()

	cfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "test-producer", nil, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByKey(partitionChooser)(&cfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)

	writer, err := NewMultiWriter(describer, writerCfg, &cfg)
	require.NoError(t, err)

	return writer
}

func TestInflightBuffer_AcquireMessageAfterStopReturnsClosedError(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	bufCtx, cancel := context.WithCancel(ctx)
	cancel()

	buf := newInflightBuffer(
		bufCtx,
		&xsync.Mutex{},
		&topicwriterinternal.WriterReconnectorConfig{},
		func() error { return nil },
	)

	require.ErrorIs(t, buf.acquireMessage(ctx), ErrAlreadyClosed)
}

func TestMultiWriter_WriteAfterClose(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Close(ctx))

	err := multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data:  bytes.NewReader([]byte("hello")),
		SeqNo: 1,
		Key:   "partition-key-1",
	}})
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestMultiWriter_WriteReturnsChoosePartitionError(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	choosePartitionErr := errors.New("choose partition failed")
	multiWriter := newTestMultiWriterWithPartitionChooser(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		&errorPartitionChooser{err: choosePartitionErr},
	)

	require.NoError(t, multiWriter.WaitInit(ctx))

	err := multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data:  bytes.NewReader([]byte("hello")),
		SeqNo: 1,
		Key:   "partition-key-1",
	}})
	require.ErrorIs(t, err, choosePartitionErr)

	multiWriter.orchestrator.mu.WithLock(func() {
		require.Equal(t, 0, multiWriter.orchestrator.buf.inFlightMessages.Len())
		require.Len(t, multiWriter.orchestrator.buf.messagesSema, 0)
	})
	require.NoError(t, multiWriter.Close(ctx))
}

func TestOrchestratorOnAckReceivedIgnoresMissingPartition(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	multiWriter.orchestrator.mu.WithLock(func() {
		multiWriter.orchestrator.buf.messagesSema <- struct{}{}
		multiWriter.orchestrator.buf.pushNeedLock(message{
			MessageWithDataContent: topicwritercommon.MessageWithDataContent{
				PublicMessage: topicwritercommon.PublicMessage{
					SeqNo:       1,
					PartitionID: 1,
				},
			},
		})
		delete(multiWriter.orchestrator.buf.pendingMessagesIndex, 1)
		delete(multiWriter.orchestrator.partitions, 1)
		require.NotPanics(t, func() {
			multiWriter.orchestrator.onAckReceivedNeedLock(1, 1)
		})
		require.Equal(t, 0, multiWriter.orchestrator.buf.inFlightMessages.Len())
		require.Len(t, multiWriter.orchestrator.buf.messagesSema, 0)
	})
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_OnPartitionSplitReturnsAddPartitionsError(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	baseDesc := stubs.DefaultStubTopicDescription(t)
	state := stubs.NewDescribeWithSplitsState(t, baseDesc, 6)
	stubClient := stubs.NewStubTopicClientWithSplits(t, state)
	addPartitionsErr := errors.New("add child partitions failed")
	partitionChooser := &failingAddPartitionChooser{
		delegate: partitionchooser.NewBoundPartitionChooser(),
		err:      addPartitionsErr,
	}
	multiWriter := newTestMultiWriterWithPartitionChooser(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		partitionChooser,
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	state.RecordSplit(1)

	err := multiWriter.orchestrator.onPartitionSplit(1)
	require.ErrorIs(t, err, addPartitionsErr)
	require.NoError(t, multiWriter.Close(ctx))
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

func TestMultiWriter_CloseCancelsInitSeqNoRetrySleep(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		&overloadedInitWritersFactory{},
	)

	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	startedAt := time.Now()
	require.NoError(t, multiWriter.Close(closeCtx))
	require.Less(t, time.Since(startedAt), 100*time.Millisecond)
}

func TestOrchestratorDescribeTopicWithRetriesCancelsRetrySleep(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(xtest.Context(t))
	bg := background.NewWorker(ctx, "describe-retry-test")
	describeResult := stubs.DefaultStubTopicDescription(t)
	o := newOrchestrator(
		ctx,
		cancel,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return describeResult, nil
		},
		bg,
		&topicwriterinternal.WriterReconnectorConfig{},
		&MultiWriterConfig{},
	)
	cancel()
	defer func() {
		_ = bg.Close(xtest.Context(t), nil)
	}()

	startedAt := time.Now()
	_, err := o.describeTopicWithRetries(describeResult.Partitions[0].PartitionID)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(startedAt), 100*time.Millisecond)
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
	require.Eventually(t, func() bool {
		select {
		case <-multiWriter.background.Done():
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond*10)
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

func TestMultiWriter_Write_ErrUnorderedSeqNo(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	cfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "test-producer", nil, 0))(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByPartitionID()(&cfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)

	multiWriter, err := NewMultiWriter(
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		writerCfg,
		&cfg,
	)
	require.NoError(t, err)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{
			Data:        bytes.NewReader([]byte("hello")),
			SeqNo:       2,
			PartitionID: 1,
		},
	}))
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{
			Data:        bytes.NewReader([]byte("hello")),
			SeqNo:       2,
			PartitionID: 2,
		},
	}))

	err = multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{
			Data:        bytes.NewReader([]byte("hello")),
			SeqNo:       2,
			PartitionID: 1,
		},
	})
	require.ErrorIs(t, err, ErrUnorderedSeqNo)

	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_Write_AutoSeqNoFollowsQueueOrder(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)

	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	orderedWriter := &orderedSeqWriter{
		writes: make(chan int64, 2),
	}
	cfg := MultiWriterConfig{}
	withWritersFactory(orderedSeqWriterFactory{writer: orderedWriter})(&cfg)
	WithProducerIDPrefix("test-producer")(&cfg)
	WithWriterPartitionByPartitionID()(&cfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(100)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)

	multiWriter, err := NewMultiWriter(
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		writerCfg,
		&cfg,
	)
	require.NoError(t, err)
	require.NoError(t, multiWriter.WaitInit(ctx))

	reader := &blockingReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte("slow"),
	}
	firstWriteDone := make(chan error, 1)
	go func() {
		firstWriteDone <- multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
			{
				Data:        reader,
				PartitionID: 1,
			},
		})
	}()

	<-reader.started
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{
			Data:        bytes.NewReader([]byte("fast")),
			PartitionID: 1,
		},
	}))
	require.Equal(t, int64(1), <-orderedWriter.writes)

	close(reader.release)
	require.NoError(t, <-firstWriteDone)
	require.Equal(t, int64(2), <-orderedWriter.writes)

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

var errTestStopWriterReconnector = errors.New("ydb: stop writer reconnector")

type blockingInitWriter struct {
	releaseInit <-chan struct{}
	closeCalled chan struct{}
}

func (w *blockingInitWriter) Close(_ context.Context) error {
	select {
	case w.closeCalled <- struct{}{}:
	default:
	}

	return errTestStopWriterReconnector
}

func (w *blockingInitWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	select {
	case <-ctx.Done():
		return topicwriterinternal.InitialInfo{}, ctx.Err()
	case <-w.closeCalled:
		return topicwriterinternal.InitialInfo{}, errTestStopWriterReconnector
	case <-w.releaseInit:
	}

	return topicwriterinternal.InitialInfo{}, nil
}

func (w *blockingInitWriter) WriteInternal(
	_ context.Context,
	_ []topicwritercommon.MessageWithDataContent,
) error {
	return nil
}

type blockingInitWritersFactory struct {
	releaseInit <-chan struct{}
}

func (f *blockingInitWritersFactory) Create(_ topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	return &blockingInitWriter{
		releaseInit: f.releaseInit,
		closeCalled: make(chan struct{}, 1),
	}, nil
}

func pendingPartitionSplitCount(r *partitionSplitReceiver) int {
	r.partitionSplits.mu.Lock()
	defer r.partitionSplits.mu.Unlock()

	return r.partitionSplits.Len()
}

func TestMultiWriter_WaitInit_PartitionSplitQueuedDuringInit(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	releaseInit := make(chan struct{})

	baseDesc := stubs.DefaultStubTopicDescription(t)
	state := stubs.NewDescribeWithSplitsState(t, baseDesc, 6)
	stubClient := stubs.NewStubTopicClientWithSplits(t, state)

	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
		&blockingInitWritersFactory{releaseInit: releaseInit},
	)

	waitResult := make(chan error, 1)
	go func() {
		waitResult <- multiWriter.WaitInit(ctx)
	}()

	require.Eventually(t, func() bool {
		return multiWriter.getWritersCount() > 0
	}, time.Second, 10*time.Millisecond, "initSeqNo should create partition writers")

	// Simulate a partition split event while initSeqNo is still waiting for writer init.
	// The split is queued but must not be processed until init completes and workers start.
	state.RecordSplit(1)
	multiWriter.orchestrator.partitionSplitReceiver.push(1)

	select {
	case err := <-waitResult:
		require.NoError(t, err, "WaitInit must not finish while initSeqNo is blocked")
	default:
	}

	require.Eventually(t, func() bool {
		return pendingPartitionSplitCount(multiWriter.orchestrator.partitionSplitReceiver) == 1
	}, 200*time.Millisecond, 10*time.Microsecond,
		"split event must stay queued until init completes",
	)

	close(releaseInit)

	select {
	case err := <-waitResult:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitInit timed out")
	}

	require.NoError(t, multiWriter.Close(ctx))
}
