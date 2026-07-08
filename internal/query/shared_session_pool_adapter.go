package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type sessionPoolAdapter struct {
	shared *SessionPool
	cfg    *config.Config
}

func (a *sessionPoolAdapter) Stats() pool.Stats {
	return a.shared.Stats()
}

func (a *sessionPoolAdapter) Close(ctx context.Context) error {
	// The driver owns the shared pool lifecycle.
	return nil
}

func (a *sessionPoolAdapter) With(
	ctx context.Context,
	f func(ctx context.Context, s *Session) error,
	opts ...retry.Option,
) error {
	return a.shared.WithCore(ctx, func(ctx context.Context, core Core) error {
		s := sessionFromCore(core, a.cfg)

		if err := f(ctx, s); err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, opts...)
}

func sessionFromCore(core Core, cfg *config.Config) *Session {
	sc, _ := core.(*sessionCore)

	return &Session{
		Core:                     core,
		client:                   sc.Client,
		trace:                    sc.Trace,
		lazyTx:                   cfg.LazyTx(),
		streamResultCloseTimeout: sc.deleteTimeout,
	}
}
