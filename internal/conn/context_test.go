package conn

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestWithoutWrapping(t *testing.T) {
	t.Run("ContextWithoutWrapping", func(t *testing.T) {
		ctx := context.Background()
		ctxWithoutWrapping := WithoutWrapping(ctx)
		require.NotNil(t, ctxWithoutWrapping)
		require.False(t, UseWrapping(ctxWithoutWrapping))
	})
}

func TestUseWrapping(t *testing.T) {
	t.Run("DefaultContextUsesWrapping", func(t *testing.T) {
		ctx := context.Background()
		require.True(t, UseWrapping(ctx))
	})

	t.Run("ContextWithoutWrappingDoesNotUseWrapping", func(t *testing.T) {
		ctx := WithoutWrapping(context.Background())
		require.False(t, UseWrapping(ctx))
	})

	t.Run("NestedContextWithoutWrapping", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithoutWrapping(ctx)
		ctx = context.WithValue(ctx, "key", "value") //nolint:revive,staticcheck
		require.False(t, UseWrapping(ctx))
	})
}

func TestCheckErrToBan(t *testing.T) {
	ctx := BanOnOperationError(t.Context(), Ydb.StatusIds_ABORTED)
	require.False(t, CheckErrForBan(ctx, nil))
	require.False(t, CheckErrForBan(ctx, errors.New("test")))
	require.False(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)))))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED)))))
	ctx = BanOnOperationError(ctx, Ydb.StatusIds_OVERLOADED)
	require.False(t, CheckErrForBan(ctx, nil))
	require.False(t, CheckErrForBan(ctx, errors.New("test")))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)))))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED)))))
	ctx = BanOnTransportError(ctx, grpcCodes.Aborted)
	require.False(t, CheckErrForBan(ctx, nil))
	require.False(t, CheckErrForBan(ctx, errors.New("test")))
	require.False(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Transport(grpcStatus.New(grpcCodes.Unavailable, "test").Err()))))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Transport(grpcStatus.New(grpcCodes.Aborted, "test").Err()))))
	ctx = BanOnTransportError(ctx, grpcCodes.Unavailable)
	require.False(t, CheckErrForBan(ctx, nil))
	require.False(t, CheckErrForBan(ctx, errors.New("test")))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Transport(grpcStatus.New(grpcCodes.Unavailable, "test").Err()))))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Transport(grpcStatus.New(grpcCodes.Aborted, "test").Err()))))
	require.False(t, CheckErrForBan(ctx, nil))
	require.False(t, CheckErrForBan(ctx, errors.New("test")))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)))))
	require.True(t, CheckErrForBan(ctx, xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED)))))
}
