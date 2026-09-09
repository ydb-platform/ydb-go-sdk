package endpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEndpointMetadata(t *testing.T) {
	metadata := Metadata{
		LocalDC:         true,
		BridgePileState: PileStatePrimary,
	}
	candidate := New("node:2135", WithMetadata(metadata))

	require.Equal(t, metadata, candidate.Metadata())
	require.True(t, candidate.LocalDC())
	require.Equal(t, metadata, candidate.Copy().Metadata())

	nonLocal := candidate.Copy(WithLocalDC(false))
	require.False(t, nonLocal.LocalDC())
	require.Equal(t, PileStatePrimary, nonLocal.Metadata().BridgePileState)
	require.Contains(t, candidate.String(), "local:true")
}
