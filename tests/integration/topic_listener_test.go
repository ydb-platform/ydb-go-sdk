//go:build integration
// +build integration

package integration

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
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

	startedListener, err := scope.Driver().Topic().StartListener(
		scope.TopicConsumerName(),
		handler,
		topicoptions.ReadTopic(scope.TopicPath()),
	)
	require.NoError(t, err)
	require.NoError(t, startedListener.WaitInit(scope.Ctx))

	xtest.WaitChannelClosed(t, done)

	require.NotNil(t, readMessages)

	content := string(xtest.Must(io.ReadAll(readMessages.Batch.Messages[0])))
	require.Equal(t, "asd", content)

	require.NoError(t, startedListener.Close(scope.Ctx))
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
