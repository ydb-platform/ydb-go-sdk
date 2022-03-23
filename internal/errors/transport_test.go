package errors

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsTransportError(t *testing.T) {
	code := TransportErrorCanceled
	for _, err := range []error{
		&TransportError{Reason: code},
		&TransportError{Reason: code, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &TransportError{Reason: code}),
	} {
		t.Run("", func(t *testing.T) {
			if !IsTransportError(err, code) {
				t.Errorf("expected %v to be TransportError with code=%v", err, code)
			}
		})
	}
}

func TestIsNonTransportError(t *testing.T) {
	code := TransportErrorCanceled
	for _, err := range []error{
		&TransportError{Reason: TransportErrorAborted},
		&TransportError{Reason: TransportErrorAborted, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &TransportError{Reason: TransportErrorAborted}),
		&OperationError{Reason: StatusBadRequest},
	} {
		t.Run("", func(t *testing.T) {
			if IsTransportError(err, code) {
				t.Errorf("expected %v not to be TransportError with code=%v", err, code)
			}
		})
	}
}

func TestTransportErrorWrapsContextError(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &TransportError{
		Reason: TransportErrorCanceled,
		err:    context.Canceled,
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected %v to wrap deadline.Canceled", err)
	}
}

func TestIsNonOperationError(t *testing.T) {
	code := StatusBadRequest
	for _, err := range []error{
		&OperationError{Reason: StatusTimeout},
		fmt.Errorf("wrapped: %w", &OperationError{Reason: StatusTimeout}),
		&TransportError{Reason: TransportErrorAborted},
	} {
		t.Run("", func(t *testing.T) {
			if IsOpError(err, code) {
				t.Errorf("expected %v not to be OperationError with code=%v", err, code)
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
			error:     NewTransportError(WithTEReason(TransportErrorUnknownCode)),
			pessimize: true,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorCanceled)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorUnknown)),
			pessimize: true,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorInvalidArgument)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorDeadlineExceeded)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorNotFound)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorAlreadyExists)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorPermissionDenied)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorResourceExhausted)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorFailedPrecondition)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorAborted)),
			pessimize: true,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorOutOfRange)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorUnimplemented)),
			pessimize: false,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorInternal)),
			pessimize: true,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorUnavailable)),
			pessimize: true,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorDataLoss)),
			pessimize: true,
		},
		{
			error:     NewTransportError(WithTEReason(TransportErrorUnauthenticated)),
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
