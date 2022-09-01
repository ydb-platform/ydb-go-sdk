package xerrors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
)

func TestIsTransportError(t *testing.T) {
	code := grpcCodes.Canceled
	for _, err := range []error{
		&transportError{code: code},
		&transportError{code: code, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &transportError{code: code}),
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
		&transportError{code: grpcCodes.Aborted},
		&transportError{code: grpcCodes.Aborted, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &transportError{code: grpcCodes.Aborted}),
		&operationError{code: Ydb.StatusIds_BAD_REQUEST},
	} {
		t.Run("", func(t *testing.T) {
			if IsTransportError(err, code) {
				t.Errorf("expected %v not to be transportError with code=%v", err, code)
			}
		})
	}
}

func TestTransportErrorWrapsContextError(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &transportError{
		code: grpcCodes.Canceled,
		err:  context.Canceled,
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected %v to wrap deadline.Canceled", err)
	}
}

func TestIsNonOperationError(t *testing.T) {
	code := Ydb.StatusIds_BAD_REQUEST
	for _, err := range []error{
		&operationError{code: Ydb.StatusIds_TIMEOUT},
		fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_TIMEOUT}),
		&transportError{code: grpcCodes.Aborted},
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
			error:     Transport(),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.Canceled)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.Unknown)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.InvalidArgument)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.DeadlineExceeded)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.NotFound)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.AlreadyExists)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.PermissionDenied)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.ResourceExhausted)),
			pessimize: false,
		},
		{
			error:     Transport(WithCode(grpcCodes.FailedPrecondition)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.Aborted)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.OutOfRange)),
			pessimize: false,
		},
		{
			error:     Transport(WithCode(grpcCodes.Unimplemented)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.Internal)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.Unavailable)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.DataLoss)),
			pessimize: true,
		},
		{
			error:     Transport(WithCode(grpcCodes.Unauthenticated)),
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
