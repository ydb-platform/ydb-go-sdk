package badconn

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
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
		//nolint:staticcheck
		// ignore SA1019
		//nolint:nolintlint
		grpc.ErrClientConnClosing,
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
	),
	xerrors.Retryable(
		grpcStatus.Error(grpcCodes.Unavailable, ""),
		xerrors.WithBackoff(backoff.TypeFast),
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
	xerrors.Retryable(errors.New("retryable error")),
	io.EOF,
	xerrors.WithStackTrace(io.EOF),
}

func Test_badConnError_Is(t *testing.T) {
	for _, err := range errsToCheck {
		t.Run(err.Error(), func(t *testing.T) {
			err = Map(err)
			require.Equal(t,
				xerrors.MustDeleteTableOrQuerySession(err),
				xerrors.Is(err, driver.ErrBadConn),
			)
		})
	}
}

func Test_badConnError_As_Error(t *testing.T) {
	for _, err := range errsToCheck {
		t.Run(err.Error(), func(t *testing.T) {
			require.ErrorAs(t, Map(err), &err) //nolint:gosec
		})
	}
}
