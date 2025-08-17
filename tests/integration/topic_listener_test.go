//go:build integration
// +build integration

package integration

import (
	"context"
	"io"
	"strings"
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
