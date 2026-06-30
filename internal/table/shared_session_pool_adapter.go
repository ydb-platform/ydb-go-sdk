package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	balancerContext "github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type sharedSessionPoolAdapter struct {
	shared *query.SharedSessionPool
	cc     grpc.ClientConnInterface
	cfg    *config.Config
}

func newSharedSessionPoolAdapter(
	shared *query.SharedSessionPool,
	cc grpc.ClientConnInterface,
	cfg *config.Config,
) sessionPool {
	return &sharedSessionPoolAdapter{
		shared: shared,
		cc:     cc,
		cfg:    cfg,
	}
}

func (a *sharedSessionPoolAdapter) Stats() pool.Stats {
	return a.shared.Stats()
}

func (a *sharedSessionPoolAdapter) Close(ctx context.Context) error {
	// The driver owns the shared pool lifecycle.
	return nil
}

func (a *sharedSessionPoolAdapter) With(
	ctx context.Context,
	f func(ctx context.Context, s *Session) error,
	opts ...retry.Option,
) error {
	return a.shared.WithCore(ctx, func(ctx context.Context, core query.Core) error {
		s := sessionFromQueryCore(a.cc, a.cfg, core)

		if err := f(ctx, s); err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, opts...)
}

func sessionFromQueryCore(
	cc grpc.ClientConnInterface,
	cfg *config.Config,
	core query.Core,
) *Session {
	s := &Session{
		config: cfg,
		status: table.SessionReady,
		id:     core.ID(),
	}
	s.nodeID.Store(core.NodeID())

	s.client = Ydb_Table_V1.NewTableServiceClient(
		conn.WithContextModifier(cc, func(ctx context.Context) context.Context {
			return meta.WithTrailerCallback(balancerContext.WithNodeID(ctx, s.NodeID()), s.checkCloseHint)
		}),
	)

	if cfg.ExecuteDataQueryOverQueryService() {
		s.dataQuery = queryClientExecutor{
			core:   core,
			client: query.QueryServiceClientFromCore(core),
		}
	} else {
		s.dataQuery = tableClientExecutor{
			client:          s.client,
			ignoreTruncated: cfg.IgnoreTruncated(),
		}
	}

	return s
}
