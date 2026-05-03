package topiclistenerinternal

import (
	"testing"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
)

func TestStreamListenerReadSessionID(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	// Fixture sets sessionID = "test-session-id"
	require.Equal(t, "test-session-id", listener.ReadSessionID())
}

func TestTopicListenerReconnectorReadSessionID(t *testing.T) {
	t.Run("NilStreamListener", func(t *testing.T) {
		lr := &TopicListenerReconnector{}
		require.Equal(t, "", lr.ReadSessionID())
	})

	t.Run("WithStreamListener", func(t *testing.T) {
		lr := &TopicListenerReconnector{
			streamListener: &streamListener{sessionID: "test-session-id"},
		}
		require.Equal(t, "test-session-id", lr.ReadSessionID())
	})
}
