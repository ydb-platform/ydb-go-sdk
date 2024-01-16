package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type SessionPool interface {
	With(ctx context.Context, f func(ctx context.Context, s *session) error) error
	Close(ctx context.Context) error
}

var _ SessionPool = (*poolMock)(nil)

type poolMock struct {
	create func(ctx context.Context) (*session, error)
	close  func(ctx context.Context, s *session) error
}

func (pool poolMock) Close(ctx context.Context) error {
	return nil
}

func (pool poolMock) With(ctx context.Context, f func(ctx context.Context, s *session) error) error {
	s, err := pool.create(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	defer func() {
		_ = pool.close(ctx, s)
	}()
	err = f(ctx, s)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}
