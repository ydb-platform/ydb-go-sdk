package retry

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type idempotency bool

func (t idempotency) String() string {
	if t {
		return "idempotent"
	}

	return "non-idempotent"
}

const (
	idempotent    = true
	nonIdempotent = false
)

var errsToCheck = []struct {
	err           error        // given error
	backoff       backoff.Type // no backoff (=== no operationStatus), fast backoff, slow backoff
	deleteSession bool         // close session and delete from pool
	canRetry      map[idempotency]bool
}{
	{
		// retryer given unknown error - we will not operationStatus and will close session
		err:           fmt.Errorf("unknown error"),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		// golang context deadline exceeded
		err:           context.DeadlineExceeded,
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		// golang context canceled
		err:           context.Canceled,
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			//nolint:staticcheck
			// ignore SA1019
			//nolint:nolintlint
			grpc.ErrClientConnClosing,
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.Canceled, "")),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true, // if client context is not done
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.Unknown, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.InvalidArgument, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true, // if client context is not done
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.NotFound, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.AlreadyExists, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
		backoff:       backoff.TypeSlow,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.Aborted, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.OutOfRange, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.Unimplemented, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.Internal, "")),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Retryable(
			xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			xerrors.WithBackoff(backoff.TypeFast),
			xerrors.InvalidObject(),
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Retryable(
			grpcStatus.Error(grpcCodes.Unavailable, ""),
			xerrors.WithBackoff(backoff.TypeFast),
			xerrors.InvalidObject(),
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.DataLoss, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_INTERNAL_ERROR),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_ABORTED),
		),
		backoff:       backoff.TypeFast,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE),
		),
		backoff:       backoff.TypeFast,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED),
		),
		backoff:       backoff.TypeSlow,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_SCHEME_ERROR),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_TIMEOUT),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_ALREADY_EXISTS),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_CANCELLED),
		),
		backoff:       backoff.TypeFast,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_UNDETERMINED),
		),
		backoff:       backoff.TypeFast,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_UNSUPPORTED),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY),
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
}
