package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme/config"
)

type lazyScheme struct {
	db      db.Connection
	options []config.Option
	client  scheme.Client
	m       sync.Mutex
}

func Scheme(db db.Connection, options []config.Option) scheme.Client {
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
	s.init()
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return s.client.ModifyPermissions(ctx, path, opts...)
	})
}

func (s *lazyScheme) Close(ctx context.Context) (err error) {
	s.m.Lock()
	defer s.m.Unlock()
	if s.client == nil {
		return nil
	}
	defer func() {
		s.client = nil
	}()
	err = s.client.Close(ctx)
	if err != nil {
		return errors.Errorf("lazyScheme.Close(): %w", err)
	}
	return nil
}

func (s *lazyScheme) init() {
	s.m.Lock()
	if s.client == nil {
		s.client = builder.New(s.db, s.options)
	}
	s.m.Unlock()
}

func (s *lazyScheme) DescribePath(ctx context.Context, path string) (e scheme.Entry, err error) {
	s.init()
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		e, err = s.client.DescribePath(ctx, path)
		return err
	}, retry.WithIdempotent())
	return e, err
}

func (s *lazyScheme) MakeDirectory(ctx context.Context, path string) (err error) {
	s.init()
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return s.client.MakeDirectory(ctx, path)
	})
}

func (s *lazyScheme) ListDirectory(ctx context.Context, path string) (d scheme.Directory, err error) {
	s.init()
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		d, err = s.client.ListDirectory(ctx, path)
		return err
	}, retry.WithIdempotent())
	return d, err
}

func (s *lazyScheme) RemoveDirectory(ctx context.Context, path string) (err error) {
	s.init()
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return s.client.RemoveDirectory(ctx, path)
	})
}
