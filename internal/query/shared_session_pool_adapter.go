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

func (a *sessionPoolAdapter) Stats() pool.Stats { return a.shared.Stats() }

func (a *sessionPoolAdapter) Close(context.Context) error { return nil }

func (a *sessionPoolAdapter) With(ctx context.Context,
	f func(context.Context, *Session) error, opts ...retry.Option,
) error {
	return a.shared.With(ctx, func(ctx context.Context, physical *Session) error {
		s := *physical
		s.lazyTx = a.cfg.LazyTx()
		s.defaultArrowDecoder = a.cfg.DefaultArrowDecoder()
		s.streamResultCloseTimeout = a.cfg.SessionDeleteTimeout()

		err := f(ctx, &s)
		if !xerrors.IsValid(err, &s) {
			physical.SetStatus(StatusClosing)
		}

		return err
	}, opts...)
}
