package balancers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestWithNodeID(t *testing.T) {
	ctx := WithNodeID(context.Background(), 42)
	nodeID, ok := endpoint.ContextNodeID(ctx)
	require.True(t, ok)
	require.Equal(t, uint32(42), nodeID)
}
