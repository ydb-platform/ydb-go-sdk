package topiclistener

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type TopicListener struct {
	listenerReconnector *topiclistenerinternal.TopicListenerReconnector
}

func NewTopicListener(
	client *rawtopic.Client,
	config *topiclistenerinternal.StreamListenerConfig,
	handler EventHandler,
) (*TopicListener, error) {
	reconnector, err := topiclistenerinternal.NewTopicListenerReconnector(client, config, handler)
	if err != nil {
		return nil, err
	}

	res := &TopicListener{listenerReconnector: reconnector}
	if err = handler.OnReaderCreated(&ReaderReady{Listener: res}); err != nil {
		_ = res.Close(context.Background())

		return nil, err
	}

	return res, nil
}

// ReadSessionID returns the current read session identifier.
// It can be passed to Topic().CommitOffset() to avoid interrupting the read session.
// It returns an empty string before the first connection, while reconnecting,
// and after the listener stops. The session ID changes after reconnects. The returned
// value is a point-in-time snapshot; the session may start closing immediately afterwards.
func (cr *TopicListener) ReadSessionID() string {
	return cr.listenerReconnector.ReadSessionID()
}

// WaitInit waits for the first successful connection or a terminal retry error.
func (cr *TopicListener) WaitInit(ctx context.Context) error {
	return cr.listenerReconnector.WaitInit(ctx)
}

// WaitStop waits until the listener stops and all event handlers complete.
// Canceling ctx stops only this wait. Call Close to request shutdown.
func (cr *TopicListener) WaitStop(ctx context.Context) error {
	return cr.listenerReconnector.WaitStop(ctx)
}

// Close waits for listener shutdown while ctx is active. Only the first call requests
// shutdown; concurrent or later calls return an already-closed error. If ctx expires,
// shutdown continues; call WaitStop with a new context to wait for completion.
func (cr *TopicListener) Close(ctx context.Context) error {
	return cr.listenerReconnector.Close(ctx, xerrors.WithStackTrace(topiclistenerinternal.ErrUserCloseTopic))
}
