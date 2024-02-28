package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Pool interface {
	closer.Closer

	With(ctx context.Context, f func(ctx context.Context, s *Session) error) error
}

var _ Pool = (*stubPool)(nil)

type stubPool struct {
	createFunc func(ctx context.Context) (*Session, error)
	closeFunc  func(ctx context.Context, s *Session) error
}

func newStubPool(
	createFunc func(ctx context.Context) (*Session, error),
	closeFunc func(ctx context.Context, s *Session) error,
) *stubPool {
	return &stubPool{
		createFunc: createFunc,
		closeFunc:  closeFunc,
	}
}

func (pool *stubPool) Close(ctx context.Context) error {
	return nil
}

func (pool *stubPool) get(ctx context.Context) (*Session, error) {
	select {
	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())
	default:
		s, err := pool.createFunc(ctx)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return s, nil
	}
}

func (pool *stubPool) put(ctx context.Context, s *Session) {
	_ = pool.closeFunc(ctx, s)
}

func (pool *stubPool) With(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
	s, err := pool.get(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	defer func() {
		pool.put(ctx, s)
	}()
	err = f(ctx, s)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}
