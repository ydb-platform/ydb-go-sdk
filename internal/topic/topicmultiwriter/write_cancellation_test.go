package topicmultiwriter

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestMultiWriterWriteCancellationDuringSplit(t *testing.T) {
	for _, stage := range []string{"describe", "probe"} {
		t.Run(stage, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			var describes atomic.Int32
			child := &orderedSeqWriter{writes: make(chan int64, 1)}
			factory := &cancellationSplitFactory{
				overloadedSplitFactory: overloadedSplitFactory{child: child, skipSplitCallback: true},
			}
			if stage == "probe" {
				factory.probe = &splitBaselineProbe{started: started, release: release}
			}
			w, _, ctx := newMultiWriterForSplitRaceWithDescriber(t, factory, func(ctx context.Context, _ string) (
				topictypes.TopicDescription, error,
			) {
				if describes.Add(1) == 1 {
					return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
						{PartitionID: 0, Active: true, ToBound: []byte("m")},
					}}, nil
				}
				if stage == "describe" {
					once.Do(func() { close(started) })
					select {
					case <-release:
					case <-ctx.Done():
						return topictypes.TopicDescription{}, ctx.Err()
					}
				}

				return splitTopicDescription(), nil
			})
			releaseSplit := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseSplit)
			writeCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- w.Write(writeCtx, cancellationTestMessage()) }()
			<-started
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("Write did not return after cancellation during " + stage)
			}
			releaseSplit()
			require.NoError(t, w.Write(ctx, cancellationTestMessage()))
			require.Equal(t, int64(101), <-child.writes)
			require.NoError(t, w.Flush(ctx))
			require.NoError(t, w.Close(ctx))
		})
	}
}

func TestMultiWriterCancelledFirstWriteClosesIdleSession(t *testing.T) {
	factory := &cancellationInitFactory{writer: &cancellationInitWriter{
		started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}),
	}}
	w, _, ctx := newMultiWriterForSplitRace(t, factory, WithWriterIdleTimeout(time.Nanosecond))
	t.Cleanup(func() { close(factory.writer.release) })
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Write(writeCtx, cancellationTestMessage()) }()
	<-factory.writer.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-factory.writer.closed:
	case <-time.After(time.Second):
		t.Fatal("cancelled first write left a session open beyond its idle timeout")
	}
}

func TestMultiWriterCancelledWriteKeepsSessionForOtherWaiter(t *testing.T) {
	for _, cancelOther := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_other=%t", cancelOther), func(t *testing.T) {
			testCancelledWriteWithOtherWaiter(t, cancelOther)
		})
	}
}

func TestOrchestratorCancelledInitKeepsQueuedSession(t *testing.T) {
	factory := &cancellationInitFactory{writer: &cancellationInitWriter{
		started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}),
	}}
	w, _, ctx := newMultiWriterForSplitRace(t, factory, WithWriterIdleTimeout(time.Nanosecond))
	t.Cleanup(func() { close(factory.writer.release) })
	wr, err := w.orchestrator.writerPool.get(0, true)
	require.NoError(t, err)
	<-factory.writer.started
	require.NoError(t, w.orchestrator.buf.acquireMessage(ctx))
	w.orchestrator.mu.WithLock(func() {
		wr.initWaiters++
		w.orchestrator.buf.pushNeedLock(message{MessageWithDataContent: topicwritercommon.MessageWithDataContent{
			PublicMessage: topicwritercommon.PublicMessage{PartitionID: 0, SeqNo: 1},
		}})
	})
	writeCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, w.orchestrator.waitWriterInit(writeCtx, 0, wr), context.Canceled)
	require.Zero(t, w.orchestrator.writerPool.idle.getWritersCount(), "queued messages still need the session")
	current, err := w.orchestrator.writerPool.get(0, true)
	require.NoError(t, err)
	require.Same(t, wr, current)
}

func testCancelledWriteWithOtherWaiter(t *testing.T, cancelOther bool) {
	t.Helper()
	factory := &cancellationInitFactory{writer: &cancellationInitWriter{
		started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}),
		writes: make(chan int64, 1),
	}}
	w, _, ctx := newMultiWriterForSplitRace(t, factory, WithWriterIdleTimeout(time.Nanosecond))
	release := sync.OnceFunc(func() { close(factory.writer.release) })
	t.Cleanup(release)
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- w.Write(writeCtx, cancellationTestMessage()) }()
	<-factory.writer.started
	otherCtx, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()
	secondCtx := &notifyingDoneContext{Context: otherCtx, observed: make(chan struct{}, 10)}
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.Write(secondCtx, cancellationTestMessage()) }()
	for {
		<-secondCtx.observed
		var bothWaiting bool
		w.orchestrator.mu.WithLock(func() {
			w.orchestrator.writerPool.mu.WithLock(func() {
				bothWaiting = w.orchestrator.writerPool.writers[0].initWaiters == 2
			})
		})
		if bothWaiting {
			break
		}
	}
	cancel()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	require.Zero(t, w.orchestrator.writerPool.idle.getWritersCount(), "a waiting caller still needs the session")
	if cancelOther {
		cancelSecond()
		require.ErrorIs(t, <-secondDone, context.Canceled)
		select {
		case <-factory.writer.closed:
		case <-time.After(time.Second):
			t.Fatal("session stayed open after its last waiting caller cancelled")
		}

		return
	}
	release()
	require.NoError(t, <-secondDone)
	require.Equal(t, int64(1), <-factory.writer.writes)
	require.NoError(t, w.Flush(ctx))
	require.NoError(t, w.Close(ctx))
}

func cancellationTestMessage() []topicwriterinternal.PublicMessage {
	return []topicwriterinternal.PublicMessage{{Data: bytes.NewReader([]byte("message")), Key: "a"}}
}

type cancellationSplitFactory struct {
	overloadedSplitFactory

	probe *splitBaselineProbe
}

func (f *cancellationSplitFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	if _, direct := cfg.PartitionID(); !direct && f.probe != nil {
		return f.probe, nil
	}

	return f.overloadedSplitFactory.Create(cfg)
}

type cancellationInitWriter struct {
	poolTestWriter

	started chan struct{}
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
	writes  chan int64
	onAck   func(int64)
}

func (w *cancellationInitWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	close(w.started)
	select {
	case <-w.release:
		return topicwriterinternal.InitialInfo{}, nil
	case <-w.closed:
		return topicwriterinternal.InitialInfo{}, ErrAlreadyClosed
	case <-ctx.Done():
		return topicwriterinternal.InitialInfo{}, ctx.Err()
	}
}

func (w *cancellationInitWriter) Close(ctx context.Context) error {
	w.once.Do(func() { close(w.closed) })

	return w.poolTestWriter.Close(ctx)
}

func (w *cancellationInitWriter) WriteInternal(
	ctx context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) error {
	for _, msg := range messages {
		select {
		case <-w.closed:
			return ErrAlreadyClosed
		case <-ctx.Done():
			return ctx.Err()
		case w.writes <- msg.SeqNo:
			w.onAck(msg.SeqNo)
		}
	}

	return nil
}

type cancellationInitFactory struct {
	writer *cancellationInitWriter
}

func (f *cancellationInitFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	f.writer.onAck = cfg.OnAckReceivedCallback

	return f.writer, nil
}

type notifyingDoneContext struct {
	context.Context //nolint:containedctx // Observe context waits without changing cancellation behavior.

	observed chan struct{}
}

func (ctx *notifyingDoneContext) Done() <-chan struct{} {
	select {
	case ctx.observed <- struct{}{}:
	default:
	}

	return ctx.Context.Done()
}
