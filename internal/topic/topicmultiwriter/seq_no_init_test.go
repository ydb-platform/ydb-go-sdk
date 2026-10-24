package topicmultiwriter

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestMultiWriterResendsAfterRestartWithSplitAncestor(t *testing.T) {
	factory := &restartSplitFactory{
		parent: &splitBeforeAckWriter{writes: make(chan int64, 1)},
		child:  &orderedSeqWriter{writes: make(chan int64, 1)},
	}
	var describes atomic.Int32
	w, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, func(context.Context, string) (
		topictypes.TopicDescription, error,
	) {
		desc := splitTopicDescription()
		if describes.Add(1) > 1 {
			desc.Partitions[1].ChildPartitionIDs = []int64{4, 5}
			desc.Partitions = append(desc.Partitions,
				topictypes.PartitionInfo{PartitionID: 4, Active: true, ToBound: []byte("d"), ParentPartitionIDs: []int64{2}},
				topictypes.PartitionInfo{
					PartitionID: 5, Active: true, FromBound: []byte("d"), ToBound: []byte("g"), ParentPartitionIDs: []int64{2},
				},
			)
		}

		return desc, nil
	})
	require.Zero(t, w.getWritersCount(), "WaitInit must not open partition sessions")
	require.NoError(t, w.Write(ctx, cancellationTestMessage()))
	require.Equal(t, int64(1), <-factory.parent.writes)

	flushDone := make(chan error, 1)
	go func() { flushDone <- w.Flush(ctx) }()
	select {
	case seqNo := <-factory.child.writes:
		require.Equal(t, int64(1), seqNo, "resending must preserve the original SeqNo")
		factory.childAck(seqNo)
	case err := <-flushDone:
		t.Fatalf("unacknowledged message was reported as acknowledged and dropped during split: %v", err)
	}
	require.NoError(t, <-flushDone)
	require.NoError(t, w.Close(ctx))
}

func TestOrchestratorSplitUsesPartitionProducerSeqNo(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lastSeqNo int64
		wantAck   bool
	}{
		{name: "uncommitted message", lastSeqNo: 0, wantAck: false},
		{name: "committed message with lost ack", lastSeqNo: 1, wantAck: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(xtest.Context(t))
			defer cancel()
			bg := background.NewWorker(ctx, "split test")
			t.Cleanup(func() { _ = bg.Close(ctx, nil) })
			chooser := partitionchooser.NewBoundPartitionChooser(partitionchooser.WithKeyHasher(func(key string) string {
				return key
			}))
			desc := splitTopicDescription()
			desc.Partitions[1].ChildPartitionIDs = []int64{4}
			desc.Partitions = append(desc.Partitions, topictypes.PartitionInfo{
				PartitionID: 4, Active: true, ToBound: []byte("g"), ParentPartitionIDs: []int64{2},
			})
			o := newOrchestrator(ctx, cancel, func(context.Context, string) (topictypes.TopicDescription, error) {
				return desc, nil
			}, bg, &topicwriterinternal.WriterReconnectorConfig{}, &MultiWriterConfig{
				ProducerIDPrefix: "test-producer",
				PartitionChooser: chooser,
				writersFactory:   &restartSplitFactory{lastSeqNo: tc.lastSeqNo},
			})
			for _, p := range splitTopicDescription().Partitions {
				o.partitions[p.PartitionID] = &PartitionInfo{PartitionInfo: p}
			}
			require.NoError(t, chooser.AddNewPartitions(o.partitions[2].PartitionInfo))
			require.NoError(t, o.buf.acquireMessage(ctx))
			acked := false
			o.buf.pushNeedLock(message{
				MessageWithDataContent: topicwritercommon.MessageWithDataContent{
					PublicMessage: topicwritercommon.PublicMessage{PartitionID: 2, SeqNo: 1, Key: "a"},
				},
				onAckCallback: func() { acked = true },
			})

			require.NoError(t, o.onPartitionSplit(2))
			require.Equal(t, tc.wantAck, acked)
			if tc.wantAck {
				require.Zero(t, o.buf.inFlightMessages.Len())
			} else {
				require.Equal(t, 1, o.buf.inFlightMessages.Len())
				msg := o.buf.inFlightMessages.Front().Value
				require.Equal(t, int64(4), msg.PartitionID)
				require.Equal(t, int64(1), msg.SeqNo)
				require.False(t, msg.ackReceived)
				require.Equal(t, 1, o.buf.messagesToResendIndex[4].Len())
			}
		})
	}
}

func TestMultiWriterAutoSeqNoUsesOpenedSessionBaseline(t *testing.T) {
	factory := &lastSeqWritersFactory{writers: map[int64]*orderedSeqWriter{
		0: {lastSeqNo: 5, writes: make(chan int64, 2)},
		1: {lastSeqNo: 50, writes: make(chan int64, 1)},
	}}
	w, ctx := newMultiWriterForSplitRace(t, factory, WithWriterPartitionByPartitionID())
	require.Zero(t, w.getWritersCount(), "initialization must not open partition sessions")

	for _, tc := range []struct {
		partitionID int64
		wantSeqNo   int64
	}{
		{partitionID: 0, wantSeqNo: 6},
		{partitionID: 1, wantSeqNo: 51},
		{partitionID: 0, wantSeqNo: 52},
	} {
		require.NoError(t, w.Write(ctx, []topicwriterinternal.PublicMessage{{
			Data: bytes.NewReader([]byte("message")), PartitionID: tc.partitionID,
		}}))
		require.Equal(t, tc.wantSeqNo, <-factory.writers[tc.partitionID].writes)
	}
	require.EqualValues(t, 1, factory.writers[0].initCalls.Load())
	require.EqualValues(t, 1, factory.writers[1].initCalls.Load())

	require.NoError(t, w.Close(ctx))
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

func TestMultiWriterWriteReturnsWhenBackgroundWorkerStopsBeforeSessionInit(t *testing.T) {
	ctx := xtest.Context(t)
	w, _ := newMultiWriterForSplitRace(t, &poolMockFactory{})
	closeCtx, cancelClose := context.WithCancel(ctx)
	cancelClose()
	_ = w.background.Close(closeCtx, nil)

	writeCtx, cancelWrite := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelWrite()
	err := w.Write(writeCtx, cancellationTestMessage())
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestMultiWriterRetriesFirstSessionInitAfterSplit(t *testing.T) {
	child := &orderedSeqWriter{writes: make(chan int64, 1)}
	w, ctx := newMultiWriterForSplitRaceWithDescriber(t, &splitWritersFactory{child: child}, splitRaceDescriber())

	require.NoError(t, w.Write(ctx, cancellationTestMessage()))
	require.Equal(t, int64(101), <-child.writes)
}

func TestMultiWriterStopsAfterFirstSessionSplitProbeFails(t *testing.T) {
	probeErr := errors.New("parent seq no probe failed")
	probe := &failedInitWriter{err: probeErr}
	factory := &splitWritersFactory{
		probe:             probe,
		skipSplitCallback: true,
	}
	w, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, splitRaceDescriber())

	err := w.Write(ctx, cancellationTestMessage())
	require.ErrorIs(t, err, probeErr)
	require.True(t, probe.closed.Load(), "the failed split probe must be closed before returning its error")

	writeCtx, cancelWrite := context.WithTimeout(ctx, time.Second)
	defer cancelWrite()
	err = w.Write(writeCtx, cancellationTestMessage())
	require.ErrorIs(t, err, probeErr, "later writes must fail instead of waiting on the child's seq no")
}

func TestMultiWriterStopsWhenOverloadedSessionHasNoSplit(t *testing.T) {
	var describes atomic.Int32
	w, ctx := newMultiWriterForSplitRaceWithDescriber(t, &splitWritersFactory{
		skipSplitCallback: true,
	}, func(context.Context, string) (topictypes.TopicDescription, error) {
		describes.Add(1)

		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 0, Active: true, ToBound: []byte("m")},
		}}, nil
	})

	err := w.Write(ctx, cancellationTestMessage())
	require.True(t, isOperationErrorOverloaded(err), "the session error must remain visible: %v", err)
	describesAfterFailure := describes.Load()

	err = w.Write(ctx, cancellationTestMessage())
	require.True(t, isOperationErrorOverloaded(err), "later writes must preserve the session error: %v", err)
	require.Equal(t, describesAfterFailure, describes.Load(), "later writes must not repeat the failed split probe")
}

func TestMultiWriterRetriesSessionAfterInitError(t *testing.T) {
	initErr := errors.New("session init failed")
	factory := &recoveringInitFactory{
		initErr: initErr,
		ready:   &orderedSeqWriter{lastSeqNo: 5, writes: make(chan int64, 1)},
	}
	w, ctx := newMultiWriterForSplitRace(t, factory)

	err := w.Write(ctx, cancellationTestMessage())
	require.ErrorIs(t, err, initErr)

	err = w.Write(ctx, cancellationTestMessage())
	require.NoError(t, err)
	require.Equal(t, int64(6), <-factory.ready.writes)
	require.EqualValues(t, 2, factory.attempts.Load())
}

func TestMultiWriterRechoosesPartitionSplitWhileMessageContentIsRead(t *testing.T) {
	probe := &splitBaselineProbe{started: make(chan struct{}), release: make(chan struct{})}
	child := &orderedSeqWriter{writes: make(chan int64, 1)}
	factory := &splitWritersFactory{parent: &poolTestWriter{}, probe: probe, child: child}
	w, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, splitRaceDescriber())

	reader := &blockingReader{started: make(chan struct{}), release: make(chan struct{}), data: []byte("message")}
	writeCtx := &notifyingDoneContext{Context: ctx}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.Write(writeCtx, []topicwriterinternal.PublicMessage{{Data: reader, Key: "a"}})
	}()
	<-reader.started
	// Observe the next context wait after the blocked payload read finishes.
	writeCtx.observed = make(chan struct{}, 1)

	splitDone := make(chan error, 1)
	go func() { splitDone <- w.orchestrator.onPartitionSplit(0) }()
	<-probe.started
	close(reader.release)
	<-writeCtx.observed
	require.Zero(t, w.getWritersCount(), "the child session must wait for the parent probe")
	close(probe.release)
	require.NoError(t, <-splitDone)
	require.NoError(t, <-writeDone)
	require.Equal(t, int64(101), <-child.writes)
	require.True(t, probe.closed.Load())
}

func TestMultiWriterRechoosesPartitionWhenOldSessionInitIsInterruptedBySplit(t *testing.T) {
	probe := &splitBaselineProbe{started: make(chan struct{}), release: make(chan struct{})}
	child := &orderedSeqWriter{writes: make(chan int64, 1)}
	parent := newCancellationInitWriter()
	factory := &splitWritersFactory{parent: parent, probe: probe, child: child}
	w, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, splitRaceDescriber())
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.Write(ctx, cancellationTestMessage())
	}()
	<-parent.started

	splitDone := make(chan error, 1)
	go func() { splitDone <- w.orchestrator.onPartitionSplit(0) }()
	<-probe.started
	close(probe.release)
	require.NoError(t, <-splitDone)
	require.NoError(t, <-writeDone)
	require.Equal(t, int64(101), <-child.writes)
	require.True(t, probe.closed.Load())
}

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
	if w.checkError != nil {
		w.checkError(topic.PublicCheckErrorRetryArgs{Error: err})
	}

	return topicwriterinternal.InitialInfo{}, err
}

type splitWritersFactory struct {
	parent            writer
	probe             writer
	child             *orderedSeqWriter
	other             *orderedSeqWriter
	skipSplitCallback bool
}

func (f *splitWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, direct := cfg.PartitionID()
	if !direct {
		if f.probe != nil {
			return f.probe, nil
		}

		return &orderedSeqWriter{lastSeqNo: 100}, nil
	}
	if partitionID == 0 {
		if f.parent != nil {
			return f.parent, nil
		}
		w := &overloadedSplitWriter{}
		if !f.skipSplitCallback {
			w.checkError = cfg.RetrySettings.CheckError
		}

		return w, nil
	}
	w := f.child
	if partitionID == 1 {
		w = f.other
	}
	if w != nil {
		w.onAckReceivedCallback = cfg.OnAckReceivedCallback

		return w, nil
	}

	return &poolTestWriter{}, nil
}

func newMultiWriterForSplitRace(
	t *testing.T,
	factory writersFactory,
	opts ...PublicMultiWriterOption,
) (*MultiWriter, context.Context) {
	t.Helper()

	return newMultiWriterForSplitRaceWithDescriber(t, factory, func(context.Context, string) (
		topictypes.TopicDescription, error,
	) {
		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 0, Active: true, ToBound: []byte("m")},
			{PartitionID: 1, Active: true, FromBound: []byte("m")},
		}}, nil
	}, opts...)
}

func newMultiWriterForSplitRaceWithDescriber(
	t *testing.T,
	factory writersFactory,
	describer TopicDescriber,
	opts ...PublicMultiWriterOption,
) (*MultiWriter, context.Context) {
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
	for _, opt := range opts {
		opt(&multiCfg)
	}

	w, err := NewMultiWriter(describer, writerCfg, &multiCfg)
	require.NoError(t, err)
	require.NoError(t, w.WaitInit(ctx))
	t.Cleanup(func() {
		w.orchestrator.stop()
		_ = w.background.Close(context.Background(), nil)
	})

	return w, ctx
}

func splitTopicDescription() topictypes.TopicDescription {
	return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: 0, Active: true, ToBound: []byte("m"), ChildPartitionIDs: []int64{2, 3}},
		{PartitionID: 2, Active: true, ToBound: []byte("g"), ParentPartitionIDs: []int64{0}},
		{PartitionID: 3, Active: true, FromBound: []byte("g"), ToBound: []byte("m"), ParentPartitionIDs: []int64{0}},
	}}
}

func splitRaceDescriber() TopicDescriber {
	var describes atomic.Int32

	return func(context.Context, string) (topictypes.TopicDescription, error) {
		if describes.Add(1) == 1 {
			return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
				{PartitionID: 0, Active: true, ToBound: []byte("m")},
				{PartitionID: 1, Active: true, FromBound: []byte("m")},
			}}, nil
		}

		return splitTopicDescription(), nil
	}
}

type splitBeforeAckWriter struct {
	poolTestWriter

	writes chan int64
}

func (w *splitBeforeAckWriter) WriteInternal(
	_ context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) error {
	for _, msg := range messages {
		w.writes <- msg.SeqNo
	}

	return xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
}

type restartSplitFactory struct {
	parent    *splitBeforeAckWriter
	child     *orderedSeqWriter
	childAck  func(int64)
	lastSeqNo int64
}

func (f *restartSplitFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	if partitionID, direct := cfg.PartitionID(); direct {
		if partitionID == 2 {
			return f.parent, nil
		}
		if partitionID == 4 {
			f.childAck = cfg.OnAckReceivedCallback

			return f.child, nil
		}
	}
	if cfg.ProducerID() == "test-producer-0" {
		return &orderedSeqWriter{lastSeqNo: 100}, nil
	}
	if cfg.ProducerID() == "test-producer-2" {
		return &orderedSeqWriter{lastSeqNo: f.lastSeqNo}, nil
	}

	return &poolTestWriter{}, nil
}
