package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamListenerSessionErrorSkipsUninitializedConfig(t *testing.T) {
	listener := &TopicListenerReconnector{}
	listener.traceSessionError(context.Background(), errors.New("stream stopped"), "stop")
	require.True(t, suppressListenerSessionError(context.Background(), nil))
}
