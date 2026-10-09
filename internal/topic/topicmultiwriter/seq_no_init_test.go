package topicmultiwriter

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type lastSeqWritersFactory struct {
	writers map[int64]*orderedSeqWriter
}

func (f *lastSeqWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, _ := cfg.PartitionID()
	w := f.writers[partitionID]
	if w == nil {
		w = &orderedSeqWriter{writes: make(chan int64, 1)}
		f.writers[partitionID] = w
	}
	w.onAckReceivedCallback = cfg.OnAckReceivedCallback

	return w, nil
}

func TestMultiWriterAutoSeqNoUsesOpenedSessionBaseline(t *testing.T) {
	ctx := xtest.Context(t)
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	factory := &lastSeqWritersFactory{writers: map[int64]*orderedSeqWriter{
		1: {lastSeqNo: 5, writes: make(chan int64, 2)},
		2: {lastSeqNo: 50, writes: make(chan int64, 1)},
	}}
	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(10)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	multiCfg := MultiWriterConfig{}
	withWritersFactory(factory)(&multiCfg)
	WithProducerIDPrefix("test-producer")(&multiCfg)
	WithWriterPartitionByPartitionID()(&multiCfg)

	w, err := NewMultiWriter(
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return client.Describe(ctx, path)
		},
		writerCfg,
		&multiCfg,
	)
	require.NoError(t, err)
	require.NoError(t, w.WaitInit(ctx))
	require.Zero(t, w.getWritersCount(), "initialization must not open partition sessions")

	for _, tc := range []struct {
		partitionID int64
		wantSeqNo   int64
	}{
		{partitionID: 1, wantSeqNo: 6},
		{partitionID: 2, wantSeqNo: 51},
		{partitionID: 1, wantSeqNo: 52},
	} {
		require.NoError(t, w.Write(ctx, []topicwriterinternal.PublicMessage{{
			Data: bytes.NewReader([]byte("message")), PartitionID: tc.partitionID,
		}}))
		require.Equal(t, tc.wantSeqNo, <-factory.writers[tc.partitionID].writes)
	}
	require.EqualValues(t, 1, factory.writers[1].initCalls.Load())
	require.EqualValues(t, 1, factory.writers[2].initCalls.Load())

	require.NoError(t, w.Close(ctx))
}

func TestMultiWriterWaitsForParentSeqNoBeforeWritingToSplitChild(t *testing.T) {
	probe := &splitBaselineProbe{started: make(chan struct{}), release: make(chan struct{})}
	child := &orderedSeqWriter{writes: make(chan int64, 1)}
	factory := &splitBaselineFactory{probe: probe, child: child}
	describes := 0
	w, _, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, func(context.Context, string) (
		topictypes.TopicDescription, error,
	) {
		describes++
		parent := topictypes.PartitionInfo{PartitionID: 0, Active: true, ToBound: []byte("m")}
		if describes == 1 {
			return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{parent}}, nil
		}

		return splitTopicDescription(), nil
	})

	splitDone := make(chan error, 1)
	go func() { splitDone <- w.orchestrator.onPartitionSplit(0) }()
	<-probe.started

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.Write(ctx, []topicwriterinternal.PublicMessage{{
			Data: bytes.NewReader([]byte("message")), Key: "a",
		}})
	}()
	close(probe.release)
	require.NoError(t, <-splitDone)
	require.NoError(t, <-writeDone)
	require.Equal(t, int64(101), <-child.writes)
}

func TestOrchestratorDoesNotAssignSeqNoBeforeSplitBaseline(t *testing.T) {
	chooser := partitionchooser.NewByPartitionIDPartitionChooser()
	parent := &PartitionInfo{PartitionInfo: topictypes.PartitionInfo{PartitionID: 0, Active: true}}
	o := &orchestrator{
		partitions:       map[int64]*PartitionInfo{0: parent},
		partitionChooser: chooser,
	}
	require.NoError(t, o.addNewPartitions(parent, &topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: 2, Active: true, ParentPartitionIDs: []int64{0}},
	}}, 0))
	var msg message
	msg.PartitionID = 2
	retry, err := o.assignSeqNoNeedLock(&msg, true)
	require.NoError(t, err)
	require.True(t, retry, "SeqNo assignment must wait for the parent baseline")
	require.Zero(t, msg.SeqNo)
	require.Zero(t, o.currentSeqNo.value.Load())
}

type splitBaselineProbe struct {
	poolTestWriter

	started chan struct{}
	release chan struct{}
}

func (w *splitBaselineProbe) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	close(w.started)
	select {
	case <-w.release:
		return topicwriterinternal.InitialInfo{LastSeqNum: 100}, nil
	case <-ctx.Done():
		return topicwriterinternal.InitialInfo{}, ctx.Err()
	}
}

type splitBaselineFactory struct {
	probe *splitBaselineProbe
	child *orderedSeqWriter
}

func (f *splitBaselineFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, direct := cfg.PartitionID()
	if !direct && cfg.ProducerID() == "test-producer-0" {
		return f.probe, nil
	}
	if direct && partitionID == 2 {
		f.child.onAckReceivedCallback = cfg.OnAckReceivedCallback

		return f.child, nil
	}

	return &poolTestWriter{}, nil
}

func TestMultiWriterWriteReturnsWhenBackgroundWorkerStopsBeforeSessionInit(t *testing.T) {
	ctx := xtest.Context(t)
	w, _, _ := newMultiWriterForSplitRace(t, &poolMockFactory{})
	closeCtx, cancelClose := context.WithCancel(ctx)
	cancelClose()
	_ = w.background.Close(closeCtx, nil)

	writeCtx, cancelWrite := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelWrite()
	err := w.Write(writeCtx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("message")), Key: "a",
	}})
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestMultiWriterRetriesFirstSessionInitAfterSplit(t *testing.T) {
	var describes atomic.Int32
	var describeOnce sync.Once
	describeStarted := make(chan struct{})
	releaseDescribe := make(chan struct{})
	child := &orderedSeqWriter{writes: make(chan int64, 1)}
	factory := &overloadedSplitFactory{child: child}
	w, _, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, func(context.Context, string) (
		topictypes.TopicDescription, error,
	) {
		parent := topictypes.PartitionInfo{PartitionID: 0, Active: true, ToBound: []byte("m")}
		if describes.Add(1) == 1 {
			return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{parent}}, nil
		}
		describeOnce.Do(func() { close(describeStarted) })
		<-releaseDescribe

		return splitTopicDescription(), nil
	})

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.Write(ctx, []topicwriterinternal.PublicMessage{{
			Data: bytes.NewReader([]byte("message")), Key: "a",
		}})
	}()
	<-describeStarted
	var prematureErr error
	var premature bool
	select {
	case prematureErr = <-writeDone:
		premature = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseDescribe)
	if premature {
		t.Fatalf("write returned before the split was processed: %v", prematureErr)
	}
	require.NoError(t, <-writeDone)
	require.Equal(t, int64(101), <-child.writes)
}

func TestMultiWriterStopsAfterFirstSessionSplitProbeFails(t *testing.T) {
	probeErr := errors.New("parent seq no probe failed")
	factory := &overloadedSplitFactory{
		probe:             &failedInitWriter{err: probeErr},
		skipSplitCallback: true,
	}
	var describes atomic.Int32
	w, _, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, func(context.Context, string) (
		topictypes.TopicDescription, error,
	) {
		if describes.Add(1) == 1 {
			return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
				{PartitionID: 0, Active: true, ToBound: []byte("m")},
			}}, nil
		}

		return splitTopicDescription(), nil
	})

	err := w.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("first")), Key: "a",
	}})
	require.ErrorIs(t, err, probeErr)

	writeCtx, cancelWrite := context.WithTimeout(ctx, time.Second)
	defer cancelWrite()
	err = w.Write(writeCtx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("second")), Key: "a",
	}})
	require.ErrorIs(t, err, probeErr, "later writes must fail instead of waiting on the child's seq no")
}

func TestMultiWriterStopsWhenOverloadedSessionHasNoSplit(t *testing.T) {
	var describes atomic.Int32
	w, _, ctx := newMultiWriterForSplitRaceWithDescriber(t, &overloadedSplitFactory{
		skipSplitCallback: true,
	}, func(context.Context, string) (topictypes.TopicDescription, error) {
		describes.Add(1)

		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 0, Active: true, ToBound: []byte("m")},
		}}, nil
	})

	err := w.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("first")), Key: "a",
	}})
	require.True(t, isOperationErrorOverloaded(err), "the session error must remain visible: %v", err)
	describesAfterFailure := describes.Load()

	err = w.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("second")), Key: "a",
	}})
	require.True(t, isOperationErrorOverloaded(err), "later writes must preserve the session error: %v", err)
	require.Equal(t, describesAfterFailure, describes.Load(), "later writes must not repeat the failed split probe")
}

func TestMultiWriterRetriesSessionAfterInitError(t *testing.T) {
	initErr := errors.New("session init failed")
	factory := &recoveringInitFactory{
		initErr: initErr,
		ready:   &orderedSeqWriter{lastSeqNo: 5, writes: make(chan int64, 1)},
	}
	w, _, ctx := newMultiWriterForSplitRace(t, factory)

	err := w.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("first")), Key: "a",
	}})
	require.ErrorIs(t, err, initErr)

	err = w.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("second")), Key: "a",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(6), <-factory.ready.writes)
	require.EqualValues(t, 2, factory.attempts.Load())
}

type recoveringInitFactory struct {
	initErr  error
	ready    *orderedSeqWriter
	attempts atomic.Int32
}

func (f *recoveringInitFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, direct := cfg.PartitionID()
	if !direct || partitionID != 0 {
		return &poolTestWriter{}, nil
	}
	if f.attempts.Add(1) == 1 {
		return &failedInitWriter{err: f.initErr}, nil
	}
	f.ready.onAckReceivedCallback = cfg.OnAckReceivedCallback

	return f.ready, nil
}

type failedInitWriter struct {
	poolTestWriter

	err error
}

func (w *failedInitWriter) WaitInitInfo(context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{}, w.err
}

type overloadedSplitWriter struct {
	poolTestWriter

	checkError topic.PublicCheckErrorRetryFunction
}

func (w *overloadedSplitWriter) WaitInitInfo(context.Context) (topicwriterinternal.InitialInfo, error) {
	err := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	w.checkError(topic.PublicCheckErrorRetryArgs{Error: err})

	return topicwriterinternal.InitialInfo{}, err
}

type overloadedSplitFactory struct {
	child             *orderedSeqWriter
	probe             *failedInitWriter
	skipSplitCallback bool
}

func (f *overloadedSplitFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, direct := cfg.PartitionID()
	if direct && partitionID == 0 {
		if f.skipSplitCallback {
			// Exercise the caller's split path without a competing receiver event.
			return &overloadedSplitWriter{checkError: func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
				return topic.PublicRetryDecisionStop
			}}, nil
		}

		return &overloadedSplitWriter{checkError: cfg.RetrySettings.CheckError}, nil
	}
	if !direct && cfg.ProducerID() == "test-producer-0" {
		if f.probe != nil {
			return f.probe, nil
		}

		return &orderedSeqWriter{lastSeqNo: 100, writes: make(chan int64, 1)}, nil
	}
	if direct && partitionID == 2 {
		f.child.onAckReceivedCallback = cfg.OnAckReceivedCallback

		return f.child, nil
	}

	return &poolTestWriter{}, nil
}

func newMultiWriterForSplitRace(
	t *testing.T,
	factory writersFactory,
) (*MultiWriter, *partitionchooser.BoundPartitionChooser, context.Context) {
	t.Helper()

	return newMultiWriterForSplitRaceWithDescriber(t, factory, func(context.Context, string) (
		topictypes.TopicDescription, error,
	) {
		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 0, Active: true, ToBound: []byte("m")},
			{PartitionID: 1, Active: true, FromBound: []byte("m")},
		}}, nil
	})
}

func newMultiWriterForSplitRaceWithDescriber(
	t *testing.T,
	factory writersFactory,
	describer TopicDescriber,
) (*MultiWriter, *partitionchooser.BoundPartitionChooser, context.Context) {
	t.Helper()

	ctx := xtest.Context(t)
	chooser := partitionchooser.NewBoundPartitionChooser(partitionchooser.WithKeyHasher(func(key string) string {
		return key
	}))
	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(10)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	multiCfg := MultiWriterConfig{}
	withWritersFactory(factory)(&multiCfg)
	WithProducerIDPrefix("test-producer")(&multiCfg)
	WithWriterPartitionByKey(chooser)(&multiCfg)

	w, err := NewMultiWriter(describer, writerCfg, &multiCfg)
	require.NoError(t, err)
	require.NoError(t, w.WaitInit(ctx))
	t.Cleanup(func() {
		w.orchestrator.stop()
		_ = w.background.Close(context.Background(), nil)
	})

	return w, chooser, ctx
}

func splitTopicDescription() topictypes.TopicDescription {
	return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: 0, Active: true, ToBound: []byte("m"), ChildPartitionIDs: []int64{2, 3}},
		{PartitionID: 2, Active: true, ToBound: []byte("g"), ParentPartitionIDs: []int64{0}},
		{PartitionID: 3, Active: true, FromBound: []byte("g"), ToBound: []byte("m"), ParentPartitionIDs: []int64{0}},
	}}
}

func splitPartitionZero(t *testing.T, w *MultiWriter, chooser *partitionchooser.BoundPartitionChooser) {
	t.Helper()

	w.orchestrator.mu.WithLock(func() {
		parent := w.orchestrator.partitions[0]
		parent.ChildPartitionIDs = []int64{2, 3}
		children := []topictypes.PartitionInfo{
			{PartitionID: 2, Active: true, ToBound: []byte("g"), ParentPartitionIDs: []int64{0}},
			{PartitionID: 3, Active: true, FromBound: []byte("g"), ToBound: []byte("m"), ParentPartitionIDs: []int64{0}},
		}
		for _, child := range children {
			w.orchestrator.partitions[child.PartitionID] = &PartitionInfo{PartitionInfo: child}
		}
		require.NoError(t, chooser.AddNewPartitions(children...))
		chooser.RemovePartition(0)
	})
}

func requireMessageOnPartitionTwo(t *testing.T, w *MultiWriter) {
	t.Helper()

	w.orchestrator.mu.WithLock(func() {
		front := w.orchestrator.buf.inFlightMessages.Front()
		require.NotNil(t, front)
		require.Equal(t, int64(2), front.Value.PartitionID)
	})
}

func TestMultiWriterRechoosesPartitionSplitWhileMessageContentIsRead(t *testing.T) {
	factory := &poolMockFactory{}
	w, chooser, ctx := newMultiWriterForSplitRace(t, factory)

	reader := &blockingReader{started: make(chan struct{}), release: make(chan struct{}), data: []byte("message")}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.Write(ctx, []topicwriterinternal.PublicMessage{{Data: reader, Key: "a"}})
	}()
	<-reader.started

	splitPartitionZero(t, w, chooser)
	probe, err := w.orchestrator.writerPool.get(0, false)
	require.NoError(t, err)
	close(reader.release)
	require.NoError(t, <-writeDone)
	require.False(t, probe.writer.(*poolTestWriter).closed.Load(), "the seqNo probe must stay open")
	requireMessageOnPartitionTwo(t, w)
}

type splitInitWriter struct {
	poolTestWriter

	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (w *splitInitWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	close(w.started)
	select {
	case <-ctx.Done():
		return topicwriterinternal.InitialInfo{}, ctx.Err()
	case <-w.closed:
		return topicwriterinternal.InitialInfo{}, context.Canceled
	}
}

func (w *splitInitWriter) Close(ctx context.Context) error {
	w.once.Do(func() { close(w.closed) })

	return w.poolTestWriter.Close(ctx)
}

type splitInitFactory struct {
	old *splitInitWriter
}

func (f *splitInitFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	if partitionID, direct := cfg.PartitionID(); direct && partitionID == 0 {
		return f.old, nil
	}

	return &poolTestWriter{}, nil
}

func TestMultiWriterRechoosesPartitionWhenOldSessionInitIsInterruptedBySplit(t *testing.T) {
	factory := &splitInitFactory{old: &splitInitWriter{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}}
	w, chooser, ctx := newMultiWriterForSplitRace(t, factory)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.Write(ctx, []topicwriterinternal.PublicMessage{{
			Data: bytes.NewReader([]byte("message")), Key: "a",
		}})
	}()
	<-factory.old.started

	splitPartitionZero(t, w, chooser)
	probe, err := w.orchestrator.writerPool.get(0, false)
	require.NoError(t, err)
	require.NoError(t, <-writeDone)
	require.False(t, probe.writer.(*poolTestWriter).closed.Load(), "the seqNo probe must stay open")
	requireMessageOnPartitionTwo(t, w)
}
