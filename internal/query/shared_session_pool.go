package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// SessionPool owns the explicit sessions shared by table and query clients.
type SessionPool struct {
	*pool.Pool[*Session, Session]

	closed *xsync.Value[*closeState]
}

// NewSharedSessionPool creates and optionally warms the shared explicit session pool.
func NewSharedSessionPool(ctx context.Context, cc grpc.ClientConnInterface, cfg *config.Config) (*SessionPool, error) {
	p := &SessionPool{closed: xsync.NewValue(&closeState{cancels: make(map[uint64]context.CancelFunc)})}
	var err error
	p.Pool, err = newExplicitSessionPool(ctx, Ydb_Query_V1.NewQueryServiceClient(cc), cc, cfg,
		func(cancel context.CancelFunc) func() { return registerCloseCancel(p.closed, cancel) },
	)
	if err != nil {
		cancelCloseContexts(p.closed)

		return nil, err
	}

	return p, nil
}

// Close cancels pending session cleanup and closes the shared pool once.
func (p *SessionPool) Close(ctx context.Context) error {
	if p == nil || !cancelCloseContexts(p.closed) {
		return nil
	}

	return p.Pool.Close(ctx)
}

// ClientFromCore returns the query client bound to a session core.
func ClientFromCore(core Core) Ydb_Query_V1.QueryServiceClient {
	sc, ok := core.(*sessionCore)
	if !ok || sc == nil {
		return nil
	}

	return sc.Client
}
