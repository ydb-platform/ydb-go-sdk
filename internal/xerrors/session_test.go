package xerrors

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

func TestMustDeleteTableOrQuerySession(t *testing.T) {
	t.Run("True", func(t *testing.T) {
		for _, err := range []error{
			Transport(
				//nolint:staticcheck
				// ignore SA1019
				//nolint:nolintlint
				grpc.ErrClientConnClosing,
			),
			Transport(grpcStatus.Error(grpcCodes.Canceled, "")),
			Transport(grpcStatus.Error(grpcCodes.Unknown, "")),
			Transport(grpcStatus.Error(grpcCodes.InvalidArgument, "")),
			Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
			Transport(grpcStatus.Error(grpcCodes.NotFound, "")),
			Transport(grpcStatus.Error(grpcCodes.AlreadyExists, "")),
			Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
			Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
			Transport(grpcStatus.Error(grpcCodes.Aborted, "")),
			Transport(grpcStatus.Error(grpcCodes.Unimplemented, "")),
			Transport(grpcStatus.Error(grpcCodes.Internal, "")),
			Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			Retryable(
				Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
				WithBackoff(backoff.TypeFast),
			),
			Retryable(
				grpcStatus.Error(grpcCodes.Unavailable, ""),
				WithBackoff(backoff.TypeFast),
			),
			Transport(grpcStatus.Error(grpcCodes.DataLoss, "")),
			Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			Operation(
				WithStatusCode(Ydb.StatusIds_BAD_SESSION),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_SESSION_BUSY),
			),
		} {
			t.Run(err.Error(), func(t *testing.T) {
				require.True(t, MustDeleteTableOrQuerySession(err))
			})
		}
	})
	t.Run("False", func(t *testing.T) {
		for _, err := range []error{
			fmt.Errorf("unknown error"), // retryer given unknown error - we will not operationStatus and will close session
			context.DeadlineExceeded,
			context.Canceled,
			Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
			Transport(grpcStatus.Error(grpcCodes.OutOfRange, "")),
			Operation(
				WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_UNAUTHORIZED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_INTERNAL_ERROR),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_EXTERNAL_ERROR),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_ABORTED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_UNAVAILABLE),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_OVERLOADED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_SCHEME_ERROR),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_TIMEOUT),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_ALREADY_EXISTS),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_NOT_FOUND),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_CANCELLED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_UNDETERMINED),
			),
			Operation(
				WithStatusCode(Ydb.StatusIds_UNSUPPORTED),
			),
		} {
			t.Run(err.Error(), func(t *testing.T) {
				require.False(t, MustDeleteTableOrQuerySession(err))
			})
		}
	})
}
