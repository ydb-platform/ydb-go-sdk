package coordination

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestMustDeleteSession(t *testing.T) {
	t.Run("OperationErrorBadSession", func(t *testing.T) {
		err := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("OperationErrorSessionBusy", func(t *testing.T) {
		err := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("OperationErrorSessionExpired", func(t *testing.T) {
		err := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("OperationErrorUnavailable", func(t *testing.T) {
		err := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE))
		require.False(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorCanceled", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Canceled, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorUnknown", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Unknown, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorInvalidArgument", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.InvalidArgument, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorDeadlineExceeded", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorNotFound", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.NotFound, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorAlreadyExists", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.AlreadyExists, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorPermissionDenied", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.PermissionDenied, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorFailedPrecondition", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorAborted", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Aborted, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorUnimplemented", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Unimplemented, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorInternal", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Internal, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorUnavailable", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorDataLoss", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.DataLoss, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorUnauthenticated", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, ""))
		require.True(t, mustDeleteSession(err))
	})
	t.Run("TransportErrorResourceExhausted", func(t *testing.T) {
		err := xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, ""))
		require.False(t, mustDeleteSession(err))
	})
	t.Run("NoError", func(t *testing.T) {
		require.False(t, mustDeleteSession(nil))
	})
}
