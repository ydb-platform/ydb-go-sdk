package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type lazyScheme struct {
	db      database.Connection
	options []config.Option
	c       scheme.Client
	m       sync.Mutex
}

func Scheme(db database.Connection, options []config.Option) scheme.Client {
	return &lazyScheme{
		db:      db,
		options: options,
	}
}

func (s *lazyScheme) ModifyPermissions(
	ctx context.Context,
	path string,
	opts ...scheme.PermissionsOption,
) (err error) {
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return s.client().ModifyPermissions(ctx, path, opts...)
	})
}

func (s *lazyScheme) Close(ctx context.Context) (err error) {
	s.m.Lock()
	defer s.m.Unlock()
	if s.c == nil {
		return nil
	}
	err = s.c.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func (s *lazyScheme) DescribePath(ctx context.Context, path string) (e scheme.Entry, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		e, err = s.client().DescribePath(ctx, path)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true))
	return e, xerrors.WithStackTrace(err)
}

func (s *lazyScheme) MakeDirectory(ctx context.Context, path string) (err error) {
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return s.client().MakeDirectory(ctx, path)
	})
}

func (s *lazyScheme) ListDirectory(ctx context.Context, path string) (d scheme.Directory, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		d, err = s.client().ListDirectory(ctx, path)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true))
	return d, xerrors.WithStackTrace(err)
}

func (s *lazyScheme) RemoveDirectory(ctx context.Context, path string) (err error) {
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return s.client().RemoveDirectory(ctx, path)
	})
}

func (s *lazyScheme) client() scheme.Client {
	s.m.Lock()
	defer s.m.Unlock()
	if s.c == nil {
		s.c = builder.New(s.db, s.options)
	}
	return s.c
}
