//go:build integration
// +build integration

package integration

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestTopicListener(t *testing.T) {
	scope := newScope(t)
	require.NoError(t, scope.TopicWriter().Write(scope.Ctx, topicwriter.Message{Data: strings.NewReader("asd")}))

	handler := &TestTopicListener_Handler{
		done: make(empty.Chan),
	}
	listener, err := scope.Driver().Topic().StartListener(
		scope.TopicConsumerName(),
		handler,
		topicoptions.ReadTopic(scope.TopicPath()),
	)
	require.Same(t, listener, handler.listener)
	require.NoError(t, err)
	require.NoError(t, listener.WaitInit(scope.Ctx))

	<-handler.done

	require.NotNil(t, handler.onPartitionStart)
	require.NotNil(t, handler.readMessages)

	content := string(must(io.ReadAll(handler.readMessages.Batch.Messages[0])))
	require.Equal(t, "asd", content)

	require.NoError(t, listener.Close(scope.Ctx))

	require.NotNil(t, handler.onPartitionStop)
}

type TestTopicListener_Handler struct {
	topiclistener.BaseHandler

	listener         *topiclistener.TopicListener
	readMessages     *topiclistener.ReadMessages
	onPartitionStart *topiclistener.EventStartPartitionSession
	onPartitionStop  *topiclistener.EventStopPartitionSession
	done             empty.Chan
}

func (h *TestTopicListener_Handler) OnReaderCreated(event *topiclistener.ReaderReady) error {
	h.listener = event.Listener
	return nil
}

func (h *TestTopicListener_Handler) OnStartPartitionSessionRequest(
	ctx context.Context,
	event *topiclistener.EventStartPartitionSession,
) error {
	h.onPartitionStart = event
	event.Confirm()
	return nil
}

func (h *TestTopicListener_Handler) OnStopPartitionSessionRequest(
	ctx context.Context,
	event *topiclistener.EventStopPartitionSession,
) error {
	h.onPartitionStop = event
	event.Confirm()
	return nil
}

func (h *TestTopicListener_Handler) OnReadMessages(
	ctx context.Context,
	event *topiclistener.ReadMessages,
) error {
	h.readMessages = event
	close(h.done)
	return nil
}
