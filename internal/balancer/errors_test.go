package balancer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestIsBadConn(t *testing.T) {
	ctx := xtest.Context(t)

	for i, tt := range []struct {
		err           error
		goodConnCodes []grpcCodes.Code
		badConn       bool
	}{
		{
			err:     fmt.Errorf("test"),
			badConn: false,
		},
		{
			err:     xerrors.Operation(),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.OK, "")),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Canceled, "")),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unknown, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.InvalidArgument, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.NotFound, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.AlreadyExists, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Aborted, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.OutOfRange, "")),
			badConn: false,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unimplemented, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Internal, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.DataLoss, "")),
			badConn: true,
		},
		{
			err:     xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			badConn: true,
		},
		{
			err: xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			goodConnCodes: []grpcCodes.Code{
				grpcCodes.Unauthenticated,
			},
			badConn: false,
		},
	} {
		t.Run(fmt.Sprintf("%d. %v", i, tt.err), func(t *testing.T) {
			require.Equal(t, tt.badConn, IsBadConn(ctx, tt.err, tt.goodConnCodes...))
			require.Equal(t, tt.badConn, IsBadConn(ctx, xerrors.WithStackTrace(tt.err), tt.goodConnCodes...))
			require.Equal(t, tt.badConn, IsBadConn(ctx, xerrors.Retryable(tt.err), tt.goodConnCodes...))
		})
	}
}

func TestBanOnOperationError(t *testing.T) {
	ctx := xtest.Context(t)
	// Ban on operation error ABORTED only
	ctx = BanOnOperationError(ctx, Ydb.StatusIds_ABORTED)
	require.False(t, IsBadConn(ctx, nil))
	require.False(t, IsBadConn(ctx, errors.New("test")))
	require.False(t, IsBadConn(ctx, xerrors.WithStackTrace(
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))),
	))
	require.True(t, IsBadConn(ctx, xerrors.WithStackTrace(
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED))),
	))

	// Add OVERLOADED to operation error codes
	ctx = BanOnOperationError(ctx, Ydb.StatusIds_OVERLOADED)
	require.False(t, IsBadConn(ctx, nil))
	require.False(t, IsBadConn(ctx, errors.New("test")))
	require.True(t, IsBadConn(ctx, xerrors.WithStackTrace(
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))),
	))
	require.True(t, IsBadConn(ctx, xerrors.WithStackTrace(
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED))),
	))
}

func TestBanOnContextDeadlineExceeded(t *testing.T) {
	ctx := xtest.Context(t)

	require.False(t, IsBadConn(ctx, context.DeadlineExceeded))
	require.False(t, IsBadConn(ctx, xerrors.WithStackTrace(context.DeadlineExceeded)))

	ctx = BanOnContextDeadlineExceeded(ctx)
	require.False(t, IsBadConn(ctx, context.Canceled))
	require.True(t, IsBadConn(ctx, context.DeadlineExceeded))
	require.True(t, IsBadConn(ctx, xerrors.WithStackTrace(context.DeadlineExceeded)))
}

func TestBanOnSessionCreate(t *testing.T) {
	ctx := BanOnSessionCreate(xtest.Context(t))

	require.True(t, IsBadConn(ctx, xerrors.WithStackTrace(
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))),
	))
	require.True(t, IsBadConn(ctx, xerrors.WithStackTrace(
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE))),
	))
	require.False(t, IsBadConn(ctx, xerrors.WithStackTrace(
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND))),
	))
	require.True(t, IsBadConn(ctx, context.DeadlineExceeded))
	require.False(t, IsBadConn(ctx, context.Canceled))
}

func TestBadSessionPessimizesConnection(t *testing.T) {
	ctx := xtest.Context(t)

	require.True(t, IsBadConn(ctx, xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
	)))
	require.True(t, IsBadConn(ctx, xerrors.WithStackTrace(xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
	))))
	require.False(t, IsBadConn(ctx, xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND),
	)))
}
