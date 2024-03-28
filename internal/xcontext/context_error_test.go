package xcontext

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestCtxErrError(t *testing.T) {
	for _, tt := range []struct {
		err error
		str string
	}{
		{
			err: errAt(context.Canceled, 0),
			str: "'context canceled' at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestCtxErrError(context_error_test.go:19)`", //nolint:lll
		},
		{
			err: errFrom(context.DeadlineExceeded, "some.go"),
			str: "'context deadline exceeded' from `some.go`",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.str, tt.err.Error())
		})
	}
}

func TestCtxErrIs(t *testing.T) {
	for _, tt := range []struct {
		err    error
		target error
	}{
		{
			err:    errAt(context.Canceled, 0),
			target: context.Canceled,
		},
		{
			err:    errFrom(context.DeadlineExceeded, "some.go"),
			target: context.DeadlineExceeded,
		},
		{
			err:    errAt(xerrors.WithStackTrace(context.Canceled), 0),
			target: context.Canceled,
		},
		{
			err:    errFrom(xerrors.WithStackTrace(context.DeadlineExceeded), "some.go"),
			target: context.DeadlineExceeded,
		},
		{
			err:    errAt(xerrors.WithStackTrace(fmt.Errorf("%w", context.Canceled)), 0),
			target: context.Canceled,
		},
		{
			err:    errFrom(xerrors.WithStackTrace(fmt.Errorf("%w", context.DeadlineExceeded)), "some.go"),
			target: context.DeadlineExceeded,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.ErrorIs(t, tt.err, tt.target)
		})
	}
}
