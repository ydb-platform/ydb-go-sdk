package retry

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
		//nolint:nolintlint
		//nolint:staticcheck // ignore SA1019
		err:           xerrors.FromGRPCError(grpc.ErrClientConnClosing),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err:           xerrors.Transport(),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.Canceled),
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unknown),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.InvalidArgument),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.DeadlineExceeded),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.NotFound),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.AlreadyExists),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.PermissionDenied),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.ResourceExhausted),
		),
		backoff:       backoff.TypeSlow,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.FailedPrecondition),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.Aborted),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.OutOfRange),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: false,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unimplemented),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.Internal),
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unavailable),
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Retryable(
			xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unavailable),
			),
			xerrors.WithBackoff(backoff.TypeFast),
			xerrors.WithDeleteSession(),
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
			status.Error(grpcCodes.Unavailable, ""),
			xerrors.WithBackoff(backoff.TypeFast),
			xerrors.WithDeleteSession(),
		),
		backoff:       backoff.TypeFast,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    true,
			nonIdempotent: true,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.DataLoss),
		),
		backoff:       backoff.TypeNoBackoff,
		deleteSession: true,
		canRetry: map[idempotency]bool{
			idempotent:    false,
			nonIdempotent: false,
		},
	},
	{
		err: xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unauthenticated),
		),
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
			idempotent:    false,
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
