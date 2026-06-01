package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TestNodeShutdownHintPessimizesSessionNode verifies that a NodeShutdown hint on the
// attach stream bans the balancer connection to the node where the session was created.
func TestNodeShutdownHintPessimizesSessionNode(t *testing.T) {
	mockSrv := mock.Server(t, mock.WithClusterNodes(1, 2))

	var (
		banMu        sync.Mutex
		bannedNodeID uint32
		banCause     error
	)

	openCtx := t.Context()

	driver, err := ydb.Open(openCtx, mockSrv.ConnString(),
		ydb.WithAnonymousCredentials(),
		ydb.WithTraceDriver(trace.Driver{
			OnConnBan: func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
				return func(trace.DriverConnBanDoneInfo) {
					banMu.Lock()
					defer banMu.Unlock()
					bannedNodeID = info.Endpoint.NodeID()
					banCause = info.Cause
				}
			},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = driver.Close(openCtx)
	})

	ctxNode1 := endpoint.WithNodeID(openCtx, 1)

	err = driver.Query().Do(ctxNode1, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, "SELECT 42")
		if err != nil {
			return err
		}
		defer func() {
			_ = result.Close(ctx)
		}()

		mockSrv.TriggerNodeShutdown()

		require.Eventually(t, func() bool {
			banMu.Lock()
			defer banMu.Unlock()

			return bannedNodeID == 1 && banCause != nil
		}, time.Second, 10*time.Millisecond,
			"NodeShutdown hint must pessimize the connection to the session node",
		)

		return nil
	})
	require.NoError(t, err)

	banMu.Lock()
	cause := banCause
	banMu.Unlock()
	require.ErrorContains(t, cause, "received node shutdown hint")

	// The second node must stay available for new sessions.
	err = driver.Query().Do(endpoint.WithNodeID(openCtx, 2), func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, "SELECT 42")
		if err != nil {
			return err
		}

		return result.Close(ctx)
	})
	require.NoError(t, err)
}
