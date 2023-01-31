package xerrors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestIsTransportError(t *testing.T) {
	code := grpcCodes.Canceled
	for _, err := range []error{
		&transportError{status: grpcStatus.New(code, "")},
		fmt.Errorf("wrapped: %w", &transportError{status: grpcStatus.New(code, "")}),
	} {
		t.Run("", func(t *testing.T) {
			if !IsTransportError(err, code) {
				t.Errorf("expected %v to be transportError with code=%v", err, code)
			}
		})
	}
}

func TestIsNonTransportError(t *testing.T) {
	code := grpcCodes.Canceled
	for _, err := range []error{
		&transportError{status: grpcStatus.New(grpcCodes.Aborted, "")},
		fmt.Errorf("wrapped: %w", &transportError{status: grpcStatus.New(grpcCodes.Aborted, "")}),
		&operationError{code: Ydb.StatusIds_BAD_REQUEST},
	} {
		t.Run("", func(t *testing.T) {
			if IsTransportError(err, code) {
				t.Errorf("expected %v not to be transportError with code=%v", err, code)
			}
		})
	}
}

func TestIsNonOperationError(t *testing.T) {
	code := Ydb.StatusIds_BAD_REQUEST
	for _, err := range []error{
		&operationError{code: Ydb.StatusIds_TIMEOUT},
		fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_TIMEOUT}),
		&transportError{status: grpcStatus.New(grpcCodes.Aborted, "")},
	} {
		t.Run("", func(t *testing.T) {
			if IsOperationError(err, code) {
				t.Errorf("expected %v not to be operationError with code=%v", err, code)
			}
		})
	}
}

func TestMustPessimizeEndpoint(t *testing.T) {
	for _, test := range []struct {
		error     error
		pessimize bool
	}{
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Canceled, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unknown, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.InvalidArgument, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.DeadlineExceeded, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.NotFound, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.AlreadyExists, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.PermissionDenied, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.ResourceExhausted, "")),
			pessimize: false,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.FailedPrecondition, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Aborted, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.OutOfRange, "")),
			pessimize: false,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unimplemented, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Internal, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.DataLoss, "")),
			pessimize: true,
		},
		{
			error:     Transport(grpcStatus.Error(grpcCodes.Unauthenticated, "")),
			pessimize: true,
		},
		{
			error:     context.Canceled,
			pessimize: false,
		},
		{
			error:     context.DeadlineExceeded,
			pessimize: false,
		},
		{
			error:     fmt.Errorf("user error"),
			pessimize: false,
		},
	} {
		err := errors.Unwrap(test.error)
		if err == nil {
			err = test.error
		}
		t.Run(err.Error(), func(t *testing.T) {
			pessimize := MustPessimizeEndpoint(test.error)
			if pessimize != test.pessimize {
				t.Errorf("unexpected pessimization status for error `%v`: %t, exp: %t", test.error, pessimize, test.pessimize)
			}
		})
	}
}

func TestGrpcError(t *testing.T) {
	for _, err := range []error{
		WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "")),
		WithStackTrace(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, ""))),
		WithStackTrace(WithStackTrace(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "")))),
		WithStackTrace(Transport(grpcStatus.Error(grpcCodes.Aborted, ""))),
		WithStackTrace(Transport(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "")))),
		WithStackTrace(Transport(WithStackTrace(WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, ""))))),
	} {
		t.Run(err.Error(), func(t *testing.T) {
			require.True(t, IsTransportError(err))
			s, has := grpcStatus.FromError(err)
			require.True(t, has)
			require.NotNil(t, s)
		})
	}
}
