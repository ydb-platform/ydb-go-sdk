package mock

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestConnectionEndpointMetadata(t *testing.T) {
	connection := &Conn{
		AddrField:    "node:2135",
		LocalDCField: true,
		MetadataField: endpoint.Metadata{
			BridgePileState: endpoint.PileStatePrimary,
		},
	}

	metadata := connection.Endpoint().Metadata()
	require.True(t, metadata.LocalDC)
	require.Equal(t, endpoint.PileStatePrimary, metadata.BridgePileState)
}

func TestConnectionInFlight(t *testing.T) {
	connection := &Conn{}
	require.Zero(t, connection.InFlight())
	connection.InFlightField = 3
	require.Equal(t, int64(3), connection.InFlight())
}
