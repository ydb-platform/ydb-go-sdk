//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestTopicListener(t *testing.T) {
	scope := newScope(t)

	// Write message first like commit tests do
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("asd")}))

	var readMessages *topiclistener.ReadMessages
	done := make(empty.Chan)

	handler := &TestTopicListener_Handler{
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			readMessages = event
			close(done)

			return nil
		},
	}

	startedListener := scope.TopicListener(handler)
	require.NoError(t, startedListener.WaitInit(scope.Ctx))

	xtest.WaitChannelClosed(t, done)

	require.NotNil(t, readMessages)

	content := string(xtest.Must(io.ReadAll(readMessages.Batch.Messages[0])))
	require.Equal(t, "asd", content)
}

// A retriable initial stream failure must not complete WaitInit before recovery.
func TestTopicListenerRetriesInitialConnection(t *testing.T) {
	scope := newScope(t)
	streamErr := status.Error(codes.Canceled, "initial stream interrupted")
	stopper := NewGrpcStopper(streamErr)
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))
	_ = scope.TopicPath()

	checkedErrors := make(chan error, 1)
	stopper.Stop()
	defer stopper.Start()
	listener := scope.TopicListener(&TestTopicListener_Handler{},
		topicoptions.WithListenerCheckRetryErrorFunction(
			func(args topicoptions.CheckErrorRetryArgs) topicoptions.CheckErrorRetryResult {
				select {
				case checkedErrors <- args.Error:
				default:
				}

				return topicoptions.CheckErrorRetryDecisionDefault
			},
		))

	require.ErrorIs(t, xtest.Receive(t, checkedErrors, "the initial retry decision"), streamErr)
	stopper.Start()
	require.NoError(t, listener.WaitInit(xtest.ContextWithCommonTimeout(scope.Ctx, t)))
	require.NotEmpty(t, listener.ReadSessionID())
}

// Reconnect starts a new session for the same partition.
func TestTopicListenerStartsPartitionAfterStreamCancellation(t *testing.T) {
	scope := newScope(t)
	stopper := NewGrpcStopper(status.Error(codes.Canceled, "Cancelled on the server side"))
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))

	partitionStarts := make(chan *topiclistener.EventStartPartitionSession, 1)
	scope.TopicListener(&TestTopicListener_Handler{
		onStartPartitionSessionRequest: func(ctx context.Context, event *topiclistener.EventStartPartitionSession) error {
			event.Confirm()
			select {
			case partitionStarts <- event:
			case <-ctx.Done():
			}

			return nil
		},
	}, topicoptions.WithListenerBufferSizeBytes(1))
	first := xtest.Receive(t, partitionStarts, "the initial partition session")

	stopper.Stop()
	stopper.Start()

	second := xtest.Receive(t, partitionStarts, "OnStartPartitionSessionRequest after reconnect")
	require.Equal(t, first.PartitionSession.TopicPath, second.PartitionSession.TopicPath)
	require.Equal(t, first.PartitionSession.PartitionID, second.PartitionSession.PartitionID)
	require.NotEqual(t, first.PartitionSession.PartitionSessionID, second.PartitionSession.PartitionSessionID)
}

func TestTopicListenerDoesNotReplayAcknowledgedMessageAfterReconnect(t *testing.T) {
	scope := newScope(t)
	stopper := NewGrpcStopper(status.Error(codes.Canceled, "stream interrupted"))
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx,
		topicwriter.Message{Data: strings.NewReader("first")},
	))

	type readResult struct {
		content string
		err     error
	}
	partitionStarts := make(chan *topiclistener.EventStartPartitionSession, 2)
	reads := make(chan readResult, 2)
	listener := scope.TopicListener(&TestTopicListener_Handler{
		onStartPartitionSessionRequest: func(ctx context.Context, event *topiclistener.EventStartPartitionSession) error {
			event.Confirm()
			select {
			case partitionStarts <- event:
			case <-ctx.Done():
			}

			return nil
		},
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			data, err := io.ReadAll(event.Batch.Messages[0])
			if err == nil {
				err = event.ConfirmWithAck(ctx)
			}
			select {
			case reads <- readResult{content: string(data), err: err}:
			case <-ctx.Done():
				return ctx.Err()
			}

			return err
		},
	}, topicoptions.WithListenerBufferSizeBytes(1))
	require.NoError(t, listener.WaitInit(xtest.ContextWithCommonTimeout(scope.Ctx, t)))
	firstSession := xtest.Receive(t, partitionStarts, "the initial partition session")
	firstRead := xtest.Receive(t, reads, "the acknowledged message")
	require.NoError(t, firstRead.err)
	require.Equal(t, "first", firstRead.content)

	stopper.StopOnce()
	secondSession := xtest.Receive(t, partitionStarts, "the partition session after reconnect")
	require.NotEqual(t,
		firstSession.PartitionSession.PartitionSessionID,
		secondSession.PartitionSession.PartitionSessionID,
	)

	require.NoError(t, scope.TopicWriter().Write(scope.Ctx,
		topicwriter.Message{Data: strings.NewReader("second")},
	))
	secondRead := xtest.Receive(t, reads, "the message after reconnect")
	require.NoError(t, secondRead.err)
	require.Equal(t, "second", secondRead.content, "the acknowledged message must not be replayed")
}

func TestTopicListenerReplaysUnacknowledgedMessageAndContinuesAfterReconnect(t *testing.T) {
	scope := newScope(t)
	stopper := NewGrpcStopper(status.Error(codes.Canceled, "stream interrupted"))
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx,
		topicwriter.Message{Data: strings.NewReader("first")},
	))

	type readResult struct {
		event   *topiclistener.ReadMessages
		content string
		err     error
	}
	partitionStarts := make(chan *topiclistener.EventStartPartitionSession, 2)
	reads := make(chan readResult, 3)
	var deliveryCount atomic.Int32
	listener := scope.TopicListener(&TestTopicListener_Handler{
		onStartPartitionSessionRequest: func(ctx context.Context, event *topiclistener.EventStartPartitionSession) error {
			event.Confirm()
			select {
			case partitionStarts <- event:
			case <-ctx.Done():
			}

			return nil
		},
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			data, err := io.ReadAll(event.Batch.Messages[0])
			if err == nil && deliveryCount.Add(1) > 1 {
				err = event.ConfirmWithAck(ctx)
			}
			select {
			case reads <- readResult{event: event, content: string(data), err: err}:
			case <-ctx.Done():
				return ctx.Err()
			}

			return err
		},
	}, topicoptions.WithListenerBufferSizeBytes(1))
	require.NoError(t, listener.WaitInit(xtest.ContextWithCommonTimeout(scope.Ctx, t)))
	firstSession := xtest.Receive(t, partitionStarts, "the initial partition session")
	firstRead := xtest.Receive(t, reads, "the unacknowledged message")
	require.NoError(t, firstRead.err)
	require.Equal(t, "first", firstRead.content)

	stopper.StopOnce()
	xtest.WaitChannelClosed(t, firstRead.event.Batch.Context().Done())
	secondSession := xtest.Receive(t, partitionStarts, "the partition session after reconnect")
	require.NotEqual(t,
		firstSession.PartitionSession.PartitionSessionID,
		secondSession.PartitionSession.PartitionSessionID,
	)

	replayed := xtest.Receive(t, reads, "the replayed unacknowledged message")
	require.NoError(t, replayed.err)
	require.Equal(t, "first", replayed.content)

	require.NoError(t, scope.TopicWriter().Write(scope.Ctx,
		topicwriter.Message{Data: strings.NewReader("second")},
	))
	next := xtest.Receive(t, reads, "the next message after replay")
	require.NoError(t, next.err)
	require.Equal(t, "second", next.content)
}

// Session cancellation must stop OnReadMessages calls for queued messages.
func TestTopicListenerDoesNotDeliverQueuedMessagesAfterCancellation(t *testing.T) {
	scope := newScope(t)
	stopper := NewGrpcStopper(status.Error(codes.Canceled, "Cancelled on the server side"))
	readResponses := listenerReadResponses(stopper)
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("first")}))

	partitionStarts := make(chan *topiclistener.EventStartPartitionSession, 1)
	messages := make(chan *topiclistener.ReadMessages, 3)
	awaitCancelError, cancelAwaitCancelError := context.WithCancel(scope.Ctx)
	defer cancelAwaitCancelError()
	scope.TopicListener(&TestTopicListener_Handler{
		onStartPartitionSessionRequest: func(ctx context.Context, event *topiclistener.EventStartPartitionSession) error {
			select {
			case partitionStarts <- event:
			case <-ctx.Done():
			}

			return nil
		},
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			messages <- event
			<-awaitCancelError.Done()

			return nil
		},
	})
	// Confirm only the initial session to allow message delivery.
	// Leave the session after reconnect unconfirmed so it cannot add messages to the channel.
	xtest.Receive(t, partitionStarts, "the initial partition session").Confirm()
	_ = xtest.Receive(t, readResponses, "the first read response")
	first := xtest.Receive(t, messages, "the running message handler")
	for _, data := range []string{"second", "third"} {
		require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader(data)}))
		_ = xtest.Receive(t, readResponses, "the next read response")
	}

	// Receiving the third response guarantees the second is already queued for the busy handler.
	stopper.StopOnce()
	xtest.WaitChannelClosed(t, first.Batch.Context().Done())
	cancelAwaitCancelError()

	// The preceding test checks that a new start request arrives after reconnect.
	// At this point the old stream is closed; without Confirm the new session cannot deliver messages.
	_ = xtest.Receive(t, partitionStarts, "the partition session after reconnect")
	// The channel must be empty: the old session must stop delivery, and the new one has not been confirmed.
	require.Empty(t, messages, "the canceled session must not deliver queued messages")
}

// ConfirmWithAck reports that the message belongs to an expired session.
func TestTopicListenerConfirmWithAckRejectsExpiredSession(t *testing.T) {
	scope := newScope(t)
	stopper := NewGrpcStopper(status.Error(codes.Canceled, "Cancelled on the server side"))
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("message")}))

	messages := make(chan *topiclistener.ReadMessages, 1)
	scope.TopicListener(&TestTopicListener_Handler{
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			select {
			case messages <- event:
			case <-ctx.Done():
			}

			return nil
		},
	}, topicoptions.WithListenerBufferSizeBytes(1))
	event := xtest.Receive(t, messages, "the initial message")

	stopper.Stop()
	xtest.WaitChannelClosed(t, event.Batch.Context().Done())

	// Use a live caller context to distinguish an expired session from caller cancellation.
	require.ErrorIs(t, event.ConfirmWithAck(scope.Ctx), topicreader.ErrCommitToExpiredSession)
}

// Confirm ignores send errors, but ConfirmWithAck must not report success for the failed commit.
func TestTopicListenerConfirmWithAckRejectsFailedConfirm(t *testing.T) {
	scope := newScope(t)
	stopper := NewGrpcStopper(status.Error(codes.Canceled, "Cancelled on the server side"))
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("message")}))

	handlerStarted := make(chan struct{}, 1)
	confirmResult := make(chan error, 1)
	scope.TopicListener(&TestTopicListener_Handler{
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			handlerStarted <- struct{}{}
			<-ctx.Done()
			event.Confirm()
			confirmResult <- event.ConfirmWithAck(scope.Ctx)

			return nil
		},
	})
	_ = xtest.Receive(t, handlerStarted, "OnReadMessages")

	stopper.Stop()
	require.ErrorIs(t, xtest.Receive(t, confirmResult, "ConfirmWithAck result"), topicreader.ErrCommitToExpiredSession)
}

// ConfirmWithAck must wait for the ACK even after Confirm.
func TestTopicListenerConfirmWithAckWaitsAfterConfirm(t *testing.T) {
	scope := newScope(t)
	stopper := NewGrpcStopper(status.Error(codes.Canceled, "stream interrupted"))
	confirmCtx, cancelConfirm := context.WithCancel(scope.Ctx)
	defer cancelConfirm()
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("message")}))

	resumeRecv := make(chan context.CancelFunc, 1)
	confirmResult := make(chan error, 1)
	scope.TopicListener(&TestTopicListener_Handler{
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			resume := stopper.PauseRecv()
			event.Confirm()
			resumeRecv <- resume

			err := event.ConfirmWithAck(confirmCtx)
			confirmResult <- err

			return nil
		},
	})
	resume := xtest.Receive(t, resumeRecv, "OnReadMessages after Confirm")
	defer resume()

	cancelConfirm()
	require.ErrorIs(t, xtest.Receive(t, confirmResult, "ConfirmWithAck result"), context.Canceled)
}

// An OnReadMessages error is returned by WaitStop.
func TestTopicListenerWaitStopReturnsHandlerError(t *testing.T) {
	scope := newScope(t)
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("message")}))

	handlerErr := errors.New("message handler failed")
	listener := scope.TopicListener(&TestTopicListener_Handler{
		onReadMessages: func(context.Context, *topiclistener.ReadMessages) error {
			return handlerErr
		},
	})

	require.ErrorIs(t, listener.WaitStop(xtest.ContextWithCommonTimeout(scope.Ctx, t)), handlerErr)
	require.NoError(t, listener.Close(scope.Ctx))
}

// WaitStop must not report shutdown while a partition callback is still running.
func TestTopicListenerWaitStopWaitsAfterCloseDeadline(t *testing.T) {
	scope := newScope(t)
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("message")}))

	readStarted := make(chan struct{})
	readRelease := make(chan struct{})
	readDone := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(readRelease) }) }
	defer release()

	listener := scope.TopicListener(&TestTopicListener_Handler{
		onReadMessages: func(context.Context, *topiclistener.ReadMessages) error {
			close(readStarted)
			<-readRelease
			close(readDone)

			return nil
		},
	})
	xtest.WaitChannelClosed(t, readStarted)

	closeCtx, cancelClose := context.WithTimeout(scope.Ctx, 50*time.Millisecond)
	require.ErrorIs(t, listener.Close(closeCtx), context.DeadlineExceeded)
	cancelClose()

	waitCtx, cancelWait := context.WithTimeout(scope.Ctx, 50*time.Millisecond)
	require.ErrorIs(t, listener.WaitStop(waitCtx), context.DeadlineExceeded)
	cancelWait()

	release()
	xtest.WaitChannelClosed(t, readDone)
	require.NoError(t, listener.WaitStop(xtest.ContextWithCommonTimeout(scope.Ctx, t)))
}

func TestTopicListenerCustomRetryPolicyRestartsPartition(t *testing.T) {
	scope := newScope(t)
	streamErr := status.Error(codes.PermissionDenied, "read access denied")
	stopper := NewGrpcStopper(streamErr)
	scope.Driver(ydb.With(config.WithGrpcOptions(
		grpc.WithStreamInterceptor(stopper.StreamClientInterceptor),
	)))

	partitionStarts := make(chan *topiclistener.EventStartPartitionSession, 1)
	checkedErrors := make(chan error, 1)
	scope.TopicListener(&TestTopicListener_Handler{
		onStartPartitionSessionRequest: func(ctx context.Context, event *topiclistener.EventStartPartitionSession) error {
			event.Confirm()
			select {
			case partitionStarts <- event:
			case <-ctx.Done():
			}

			return nil
		},
	}, topicoptions.WithListenerCheckRetryErrorFunction(
		func(args topicoptions.CheckErrorRetryArgs) topicoptions.CheckErrorRetryResult {
			select {
			case checkedErrors <- args.Error:
			default:
			}

			return topicoptions.CheckErrorRetryDecisionRetry
		},
	))
	first := xtest.Receive(t, partitionStarts, "the initial partition session")

	stopper.Stop()
	stopper.Start()

	require.ErrorIs(t, xtest.Receive(t, checkedErrors, "the retry policy callback"), streamErr)
	second := xtest.Receive(t, partitionStarts, "the partition session after the custom retry")
	require.Equal(t, first.PartitionSession.TopicPath, second.PartitionSession.TopicPath)
	require.Equal(t, first.PartitionSession.PartitionID, second.PartitionSession.PartitionID)
	require.NotEqual(t, first.PartitionSession.PartitionSessionID, second.PartitionSession.PartitionSessionID)
}

func TestTopicListenerCommit(t *testing.T) {
	t.Run("Commit", func(t *testing.T) {
		scope := newScope(t)

		err := scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("asd")})
		require.NoError(t, err)

		var messData string
		readed := make(empty.Chan)
		confirmed := make(empty.Chan)
		handler := &TestTopicListener_Handler{
			onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
				defer close(confirmed)

				messData = string(xtest.Must(io.ReadAll(event.Batch.Messages[0])))
				close(readed)

				event.Confirm()
				time.Sleep(time.Second / 10) // time for send the commit over tcp channel
				return nil
			},
		}

		listener, err := scope.Driver().Topic().StartListener(scope.TopicConsumerName(), handler, topicoptions.ReadTopic(scope.TopicPath()))
		require.NoError(t, err)

		xtest.WaitChannelClosed(t, readed)
		require.Equal(t, "asd", messData)

		xtest.WaitChannelClosed(t, confirmed)
		require.NoError(t, listener.Close(scope.Ctx))

		err = scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("qqq")})
		require.NoError(t, err)

		readed = make(empty.Chan)
		confirmed = make(empty.Chan)
		handler = &TestTopicListener_Handler{
			onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
				defer close(confirmed)

				messData = string(xtest.Must(io.ReadAll(event.Batch.Messages[0])))
				close(readed)

				event.Confirm()
				return nil
			},
		}

		listener, err = scope.Driver().Topic().StartListener(scope.TopicConsumerName(), handler, topicoptions.ReadTopic(scope.TopicPath()))
		require.NoError(t, err)

		xtest.WaitChannelClosed(t, readed)
		require.Equal(t, "qqq", messData)

		xtest.WaitChannelClosed(t, confirmed)
		require.NoError(t, listener.Close(scope.Ctx))
	})
	t.Run("CommitWithAck", func(t *testing.T) {
		scope := newScope(t)

		err := scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("asd")})
		require.NoError(t, err)

		var savedEvent *topiclistener.ReadMessages
		readed := make(empty.Chan)
		handler := &TestTopicListener_Handler{
			onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
				savedEvent = event
				close(readed)

				return nil
			},
		}

		listener, err := scope.Driver().Topic().StartListener(scope.TopicConsumerName(), handler, topicoptions.ReadTopic(scope.TopicPath()))
		require.NoError(t, err)

		xtest.WaitChannelClosed(t, readed)
		messData := string(xtest.Must(io.ReadAll(savedEvent.Batch.Messages[0])))
		require.Equal(t, "asd", messData)

		require.NoError(t, savedEvent.ConfirmWithAck(scope.Ctx))
		// stop listener without any waits
		closedCtx, cancel := context.WithCancel(scope.Ctx)
		cancel()
		_ = listener.Close(closedCtx)

		err = scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("qqq")})
		require.NoError(t, err)

		committed := make(empty.Chan)
		var commitError error
		handler = &TestTopicListener_Handler{
			onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
				savedEvent = event

				commitError = event.ConfirmWithAck(ctx)
				close(committed)
				return commitError
			},
		}

		listener, err = scope.Driver().Topic().StartListener(scope.TopicConsumerName(), handler, topicoptions.ReadTopic(scope.TopicPath()))
		require.NoError(t, err)

		xtest.WaitChannelClosed(t, committed)
		require.NoError(t, commitError)
		messData = string(xtest.Must(io.ReadAll(savedEvent.Batch.Messages[0])))
		require.Equal(t, "qqq", messData)

		require.NoError(t, listener.Close(scope.Ctx))
	})
}

// TestTopicListenerCommitOffsetWithSessionIDKeepsListenerAlive verifies that passing a valid
// read_session_id to CommitOffset does not interrupt the active listener session: messages
// keep arriving and the session ID remains unchanged.
func TestTopicListenerCommitOffsetWithSessionIDKeepsListenerAlive(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: strings.NewReader("msg0")},
		topicwriter.Message{Data: strings.NewReader("msg1")},
	)
	require.NoError(t, err)

	type firstMsgData struct {
		partitionID int64
		nextOffset  int64
	}

	firstMsgCh := make(chan firstMsgData, 1)
	secondMsgDone := make(empty.Chan)
	var msgCount atomic.Int32

	handler := &TestTopicListener_Handler{
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			for _, msg := range event.Batch.Messages {
				n := msgCount.Add(1)
				if n == 1 {
					firstMsgCh <- firstMsgData{
						partitionID: event.Batch.PartitionID(),
						nextOffset:  msg.Offset + 1,
					}
				} else if n == 2 {
					close(secondMsgDone)
				}
			}

			return nil
		},
	}

	listener, err := scope.Driver().Topic().StartListener(
		scope.TopicConsumerName(),
		handler,
		topicoptions.ReadTopic(scope.TopicPath()),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx) }()

	var firstMsg firstMsgData
	select {
	case firstMsg = <-firstMsgCh:
	case <-ctx.Done():
		t.Fatal("timeout waiting for first message")
	}

	sessionIDBefore := listener.ReadSessionID()
	require.NotEmpty(t, sessionIDBefore)

	err = scope.Driver().Topic().CommitOffset(
		ctx,
		scope.TopicPath(),
		firstMsg.partitionID,
		scope.TopicConsumerName(),
		firstMsg.nextOffset,
		topicoptions.WithCommitOffsetReadSessionID(sessionIDBefore),
	)
	require.NoError(t, err)

	xtest.WaitChannelClosed(t, secondMsgDone)

	// Session ID must not change — the listener was not interrupted.
	require.Equal(t, sessionIDBefore, listener.ReadSessionID())

	desc, err := scope.Driver().Topic().DescribeTopicConsumer(
		ctx,
		scope.TopicPath(),
		scope.TopicConsumerName(),
		topicoptions.IncludeConsumerStats(),
	)
	require.NoError(t, err)
	require.EqualValues(t, firstMsg.nextOffset, desc.Partitions[0].PartitionConsumerStats.CommittedOffset)
}

// TestTopicListenerCommitOffsetWithoutSessionIDReconnectsListener verifies that omitting
// read_session_id causes the server to interrupt the active session. The listener
// reconnects automatically and continues delivering messages.
func TestTopicListenerCommitOffsetWithoutSessionIDReconnectsListener(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: strings.NewReader("msg0")},
		topicwriter.Message{Data: strings.NewReader("msg1")},
	)
	require.NoError(t, err)

	type firstMsgData struct {
		partitionID int64
		nextOffset  int64
	}

	firstMsgCh := make(chan firstMsgData, 1)
	secondMsgDone := make(empty.Chan)
	var msgCount atomic.Int32

	handler := &TestTopicListener_Handler{
		onReadMessages: func(ctx context.Context, event *topiclistener.ReadMessages) error {
			for _, msg := range event.Batch.Messages {
				n := msgCount.Add(1)
				if n == 1 {
					firstMsgCh <- firstMsgData{
						partitionID: event.Batch.PartitionID(),
						nextOffset:  msg.Offset + 1,
					}
				} else if n == 2 {
					close(secondMsgDone)
				}
			}

			return nil
		},
	}

	listener, err := scope.Driver().Topic().StartListener(
		scope.TopicConsumerName(),
		handler,
		topicoptions.ReadTopic(scope.TopicPath()),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx) }()

	var firstMsg firstMsgData
	select {
	case firstMsg = <-firstMsgCh:
	case <-ctx.Done():
		t.Fatal("timeout waiting for first message")
	}

	// Commit without session ID — server will interrupt the active session.
	err = scope.Driver().Topic().CommitOffset(
		ctx,
		scope.TopicPath(),
		firstMsg.partitionID,
		scope.TopicConsumerName(),
		firstMsg.nextOffset,
	)
	require.NoError(t, err)

	// Listener must reconnect and deliver the second message.
	xtest.WaitChannelClosed(t, secondMsgDone)

	desc, err := scope.Driver().Topic().DescribeTopicConsumer(
		ctx,
		scope.TopicPath(),
		scope.TopicConsumerName(),
		topicoptions.IncludeConsumerStats(),
	)
	require.NoError(t, err)
	require.EqualValues(t, firstMsg.nextOffset, desc.Partitions[0].PartitionConsumerStats.CommittedOffset)
}

// listenerReadResponses reports topic responses containing data before passing them to the SDK.
func listenerReadResponses(stopper *GrpcStopper) <-chan struct{} {
	responses := make(chan struct{})
	stopper.onRecvMsg = func(ctx context.Context, message any) error {
		response, ok := message.(*Ydb_Topic.StreamReadMessage_FromServer)
		if ok && response.GetReadResponse().GetBytesSize() > 0 {
			select {
			case responses <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	}

	return responses
}

type TestTopicListener_Handler struct {
	topiclistener.BaseHandler

	onReaderCreated                func(event *topiclistener.ReaderReady) error
	onStartPartitionSessionRequest func(ctx context.Context, event *topiclistener.EventStartPartitionSession) error
	onStopPartitionSessionRequest  func(ctx context.Context, event *topiclistener.EventStopPartitionSession) error
	onReadMessages                 func(ctx context.Context, event *topiclistener.ReadMessages) error

	listener         *topiclistener.TopicListener
	readMessages     *topiclistener.ReadMessages
	onPartitionStart *topiclistener.EventStartPartitionSession
	onPartitionStop  *topiclistener.EventStopPartitionSession
	done             empty.Chan
}

func (h *TestTopicListener_Handler) OnReaderCreated(event *topiclistener.ReaderReady) error {
	if h.onReaderCreated == nil {
		return h.BaseHandler.OnReaderCreated(event)
	}

	return h.onReaderCreated(event)
}

func (h *TestTopicListener_Handler) OnStartPartitionSessionRequest(
	ctx context.Context,
	event *topiclistener.EventStartPartitionSession,
) error {
	if h.onStartPartitionSessionRequest == nil {
		return h.BaseHandler.OnStartPartitionSessionRequest(ctx, event)
	}

	return h.onStartPartitionSessionRequest(ctx, event)
}

func (h *TestTopicListener_Handler) OnStopPartitionSessionRequest(
	ctx context.Context,
	event *topiclistener.EventStopPartitionSession,
) error {
	if h.onStopPartitionSessionRequest == nil {
		return h.BaseHandler.OnStopPartitionSessionRequest(ctx, event)
	}

	return h.onStopPartitionSessionRequest(ctx, event)
}

func (h *TestTopicListener_Handler) OnReadMessages(
	ctx context.Context,
	event *topiclistener.ReadMessages,
) error {
	if h.onReadMessages == nil {
		return h.BaseHandler.OnReadMessages(ctx, event)
	}

	return h.onReadMessages(ctx, event)
}
