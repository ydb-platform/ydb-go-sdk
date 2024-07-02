package xcontext

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errSourceChannelClosed = xerrors.Wrap(errors.New("ydb: source channel closed"))
	errStopChannelClosed   = xerrors.Wrap(errors.New("ydb: stop channel closed"))
)

func ReadChannelWithContext[R any](ctx context.Context, c <-chan R) (res R, _ error) {
	select {
	case <-ctx.Done():
		return res, ctx.Err()
	case val, ok := <-c:
		if ok {
			return val, nil
		}
		return res, xerrors.WithStackTrace(errSourceChannelClosed)
	}
}

func ReadWithContextCause2[R any](ctx1, ctx2 context.Context, c <-chan R) (res R, _ error) {
	if ctx1.Err() != nil {
		return res, context.Cause(ctx1)
	}
	if ctx2.Err() != nil {
		return res, context.Cause(ctx2)
	}

	select {
	case <-ctx1.Done():
		return res, context.Cause(ctx1)
	case <-ctx2.Done():
		return res, context.Cause(ctx2)
	case val, ok := <-c:
		if ok {
			return val, nil
		}
		return res, xerrors.WithStackTrace(errSourceChannelClosed)
	}
}
