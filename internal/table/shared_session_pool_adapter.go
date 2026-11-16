package table

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type sessionPoolAdapter struct {
	sessionPool *query.SessionPool
	cc          grpc.ClientConnInterface
	cfg         *config.Config
}

func (a *sessionPoolAdapter) Stats() pool.Stats {
	return a.sessionPool.Stats()
}

func (a *sessionPoolAdapter) Close(context.Context) error {
	return nil
}

func (a *sessionPoolAdapter) With(
	ctx context.Context,
	f func(ctx context.Context, s *Session) error,
	opts ...retry.Option,
) error {
	return a.sessionPool.With(ctx, func(ctx context.Context, physical *query.Session) error {
		s := sessionFromQueryCore(a.cc, a.cfg, physical.Core)
		defer func() {
			if !s.IsAlive() {
				physical.SetStatus(query.StatusClosing)
			}
		}()
		if err := f(ctx, s); err != nil {
			if !xerrors.IsValid(err, s) {
				s.SetStatus(table.SessionClosing)
			}

			return xerrors.WithStackTrace(err)
		}

		return nil
	}, opts...)
}

func sessionFromQueryCore(cc grpc.ClientConnInterface, cfg *config.Config, core query.Core) *Session {
	s := &Session{config: cfg, status: table.SessionReady, borrowed: true}
	initQuerySession(s, cc, core)

	return s
}
