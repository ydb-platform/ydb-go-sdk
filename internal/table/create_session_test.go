package table

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"

	internalconn "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var errTableStateSettingConnNotImplemented = errors.New("not implemented")

// tableStateSettingConn is a test helper that implements grpc.ClientConnInterface and conn.StateSetter.
// When Invoke is called, it returns the configured invokeErr.
// SetState calls are recorded to verify pessimization behavior.
type tableStateSettingConn struct {
	mu        sync.Mutex
	lastState internalconn.State
	invokeErr error
}

func (c *tableStateSettingConn) SetState(_ context.Context, s internalconn.State) internalconn.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastState = s

	return s
}

func (c *tableStateSettingConn) state() internalconn.State {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastState
}

func (c *tableStateSettingConn) Invoke(
	_ context.Context, _ string, _ any, _ any, _ ...grpc.CallOption,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.invokeErr
}

func (c *tableStateSettingConn) NewStream(
	_ context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errTableStateSettingConnNotImplemented
}

func TestNewSessionBansConnectionOnOverloaded(t *testing.T) {
	ctx := xtest.Context(t)

	t.Run("BansConnectionOnOverloadedCreateSession", func(t *testing.T) {
		cc := &tableStateSettingConn{
			invokeErr: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
		}

		c := New(ctx, cc, config.New())
		defer func() { _ = c.Close(ctx) }()

		ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		_ = c.Do(ctxTimeout, func(ctx context.Context, s table.Session) error {
			return nil
		})

		require.Equal(t, internalconn.Banned, cc.state())
	})

	t.Run("DoesNotBanConnectionOnNonOverloadedError", func(t *testing.T) {
		cc := &tableStateSettingConn{
			invokeErr: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		}

		c := New(ctx, cc, config.New())
		defer func() { _ = c.Close(ctx) }()

		ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		_ = c.Do(ctxTimeout, func(ctx context.Context, s table.Session) error {
			return nil
		})

		require.NotEqual(t, internalconn.Banned, cc.state())
	})
}
