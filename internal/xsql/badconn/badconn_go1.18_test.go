//go:build go1.18
// +build go1.18

package badconn

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errsToCheck = []error{
	fmt.Errorf("unknown error"),
	context.DeadlineExceeded,
	context.Canceled,
	xerrors.Transport(
		grpc.ErrClientConnClosing, //nolint:staticcheck // ignore SA1019
	),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Canceled, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Unknown, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.InvalidArgument, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.NotFound, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.AlreadyExists, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Aborted, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.OutOfRange, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Unimplemented, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Internal, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
	xerrors.Retryable(
		xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
		xerrors.WithBackoff(backoff.TypeFast),
		xerrors.WithDeleteSession(),
	),
	xerrors.Retryable(
		grpcStatus.Error(grpcCodes.Unavailable, ""),
		xerrors.WithBackoff(backoff.TypeFast),
		xerrors.WithDeleteSession(),
	),
	xerrors.Transport(grpcStatus.Error(grpcCodes.DataLoss, "")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_INTERNAL_ERROR),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_ABORTED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_SCHEME_ERROR),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_TIMEOUT),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_ALREADY_EXISTS),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_CANCELLED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_UNDETERMINED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_UNSUPPORTED),
	),
	xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY),
	),
}

func Test_badConnError_Is(t *testing.T) {
	for _, err := range errsToCheck {
		t.Run(err.Error(), func(t *testing.T) {
			e := Map(err)

			if xerrors.Is(e, driver.ErrBadConn) {
				require.True(t, xerrors.Is(e, err))
			}

			if errors.Is(e, driver.ErrBadConn) {
				require.True(t, errors.Is(e, err))
			}
		})
	}
}

func Test_badConnError_As_Error(t *testing.T) {
	for _, err := range errsToCheck {
		t.Run(err.Error(), func(t *testing.T) {
			var e xerrors.Error
			if !xerrors.As(err, &e) {
				t.Skip()
			}

			require.True(t, xerrors.As(Map(err), &e))
		})
	}
}
