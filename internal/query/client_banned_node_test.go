package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientMustDeleteSessionOnBannedNode(t *testing.T) {
	client := &Client{}
	const nodeID uint32 = 42

	client.bannedNodes.Store(nodeID, struct{}{})

	s := &Session{Core: &sessionCore{nodeID: nodeID}}
	s.SetStatus(StatusIdle)

	require.True(t, client.mustDeleteSession(s, nil))

	client.OnConnAllowed(nodeID)
	require.False(t, client.mustDeleteSession(s, nil))
}
