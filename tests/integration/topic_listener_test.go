//go:build integration
// +build integration

package integration

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"io"
	"strings"
	"testing"
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
	onPartitionStart *topiclistener.StartPartitionSessionRequest
	onPartitionStop  *topiclistener.StopPartitionSessionRequest
	done             empty.Chan
}

func (h *TestTopicListener_Handler) OnReaderCreated(req topiclistener.ReaderReady) error {
	h.listener = req.Listener
	return nil
}

func (h *TestTopicListener_Handler) OnStartPartitionSessionRequest(
	ctx context.Context,
	req topiclistener.StartPartitionSessionRequest,
) (topiclistener.StartPartitionSessionResponse, error) {
	h.onPartitionStart = &req
	return topiclistener.StartPartitionSessionResponse{}, nil
}

func (h *TestTopicListener_Handler) OnStopPartitionSessionRequest(
	ctx context.Context,
	req topiclistener.StopPartitionSessionRequest,
) (topiclistener.StopPartitionSessionResponse, error) {
	h.onPartitionStop = &req
	return topiclistener.StopPartitionSessionResponse{}, nil
}

func (h *TestTopicListener_Handler) OnReadMessages(
	ctx context.Context,
	req topiclistener.ReadMessages,
) error {
	h.readMessages = &req
	close(h.done)
	return nil
}

func must[R any](res R, err error) R {
	if err != nil {
		panic(err)
	}
	return res
}
