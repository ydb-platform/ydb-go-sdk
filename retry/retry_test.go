package retry

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestRetryModes(t *testing.T) {
	type CanRetry struct {
		idempotentOperation    bool // after an error we must retry idempotent operation or no
		nonIdempotentOperation bool // after an error we must retry non-idempotent operation or no
	}
	type Case struct {
		err           error        // given error
		backoff       backoff.Type // no backoff (=== no operationStatus), fast backoff, slow backoff
		deleteSession bool         // close session and delete from pool
		canRetry      CanRetry
	}
	errs := []Case{
		{
			// retryer given unknown error - we will not operationStatus and will close session
			err:           fmt.Errorf("unknown error"),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// golang context deadline exceeded
			err:           context.DeadlineExceeded,
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// golang context canceled
			err:           context.Canceled,
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			//nolint:staticcheck
			// ignore SA1019
			// We want to check internal grpc error on chaos monkey testing
			//nolint:nolintlint
			err:           xerrors.FromGRPCError(grpc.ErrClientConnClosing),
			backoff:       backoff.TypeFast,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err:           xerrors.Transport(),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Canceled),
			),
			backoff:       backoff.TypeFast,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unknown),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.InvalidArgument),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.DeadlineExceeded),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.NotFound),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.AlreadyExists),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.PermissionDenied),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.ResourceExhausted),
			),
			backoff:       backoff.TypeSlow,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.FailedPrecondition),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Aborted),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.OutOfRange),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unimplemented),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Internal),
			),
			backoff:       backoff.TypeFast,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unavailable),
			),
			backoff:       backoff.TypeFast,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
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
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
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
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.DataLoss),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unauthenticated),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_INTERNAL_ERROR),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_ABORTED),
			),
			backoff:       backoff.TypeFast,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE),
			),
			backoff:       backoff.TypeFast,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED),
			),
			backoff:       backoff.TypeSlow,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_SCHEME_ERROR),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_TIMEOUT),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_ALREADY_EXISTS),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_CANCELLED),
			),
			backoff:       backoff.TypeFast,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNDETERMINED),
			),
			backoff:       backoff.TypeFast,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNSUPPORTED),
			),
			backoff:       backoff.TypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY),
			),
			backoff:       backoff.TypeFast,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
	}
	for i, test := range errs {
		t.Run(strconv.Itoa(i)+": "+test.err.Error(), func(t *testing.T) {
			m := Check(test.err)
			if m.MustRetry(true) != test.canRetry.idempotentOperation {
				t.Errorf(
					"unexpected must retry idempotent operation status: %v, want: %v",
					m.MustRetry(true),
					test.canRetry.idempotentOperation,
				)
			}
			if m.MustRetry(false) != test.canRetry.nonIdempotentOperation {
				t.Errorf(
					"unexpected must retry non-idempotent operation status: %v, want: %v",
					m.MustRetry(false),
					test.canRetry.nonIdempotentOperation,
				)
			}
			if m.BackoffType() != test.backoff {
				t.Errorf(
					"unexpected backoff status: %v, want: %v",
					m.BackoffType(),
					test.backoff,
				)
			}
			if m.MustDeleteSession() != test.deleteSession {
				t.Errorf(
					"unexpected delete session status: %v, want: %v",
					m.MustDeleteSession(),
					test.deleteSession,
				)
			}
		})
	}
}

type CustomError struct {
	Err error
}

func (e *CustomError) Error() string {
	return fmt.Sprintf("custom error: %v", e.Err)
}

func (e *CustomError) Unwrap() error {
	return e.Err
}

func TestRetryWithCustomErrors(t *testing.T) {
	var (
		limit = 10
		ctx   = context.Background()
	)
	for _, test := range []struct {
		error     error
		retriable bool
	}{
		{
			error: &CustomError{
				Err: RetryableError(
					fmt.Errorf("custom error"),
					WithDeleteSession(),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: xerrors.Operation(
					xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					xerrors.Operation(
						xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
					),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					xerrors.Operation(
						xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED),
					),
				),
			},
			retriable: false,
		},
	} {
		t.Run(test.error.Error(), func(t *testing.T) {
			i := 0
			err := Retry(ctx, func(ctx context.Context) (err error) {
				i++
				if i < limit {
					return test.error
				}
				return nil
			})
			if test.retriable {
				if i != limit {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
			} else {
				if i != 1 {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
			}
		})
	}
}
