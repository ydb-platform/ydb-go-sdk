package ydb

import (
	"context"
	"errors"
	"fmt"
	errors2 "github.com/ydb-platform/ydb-go-sdk/v3/errors"
	"testing"
)

func TestIsTransportError(t *testing.T) {
	code := errors2.TransportErrorCanceled
	for _, err := range []error{
		&errors2.TransportError{Reason: code},
		&errors2.TransportError{Reason: code, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &errors2.TransportError{Reason: code}),
	} {
		t.Run("", func(t *testing.T) {
			if !errors2.IsTransportError(err, code) {
				t.Errorf("expected %v to be TransportError with code=%v", err, code)
			}
		})
	}
}

func TestIsNotTransportError(t *testing.T) {
	code := errors2.TransportErrorCanceled
	for _, err := range []error{
		&errors2.TransportError{Reason: errors2.TransportErrorAborted},
		&errors2.TransportError{Reason: errors2.TransportErrorAborted, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &errors2.TransportError{Reason: errors2.TransportErrorAborted}),
		&errors2.OpError{Reason: errors2.StatusBadRequest},
	} {
		t.Run("", func(t *testing.T) {
			if errors2.IsTransportError(err, code) {
				t.Errorf("expected %v not to be TransportError with code=%v", err, code)
			}
		})
	}
}

func TestTransportErrorWrapsContextError(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &errors2.TransportError{
		Reason: errors2.TransportErrorCanceled,
		err:    context.Canceled,
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected %v to wrap context.Canceled", err)
	}
}

func TestIsOpError(t *testing.T) {
	code := errors2.StatusBadRequest
	for _, err := range []error{
		&errors2.OpError{Reason: code},
		fmt.Errorf("wrapped: %w", &errors2.OpError{Reason: code}),
	} {
		t.Run("", func(t *testing.T) {
			if !errors2.IsOpError(err, code) {
				t.Errorf("expected %v to be OpError with code=%v", err, code)
			}
		})
	}
}

func TestIsNotOpError(t *testing.T) {
	code := errors2.StatusBadRequest
	for _, err := range []error{
		&errors2.OpError{Reason: errors2.StatusTimeout},
		fmt.Errorf("wrapped: %w", &errors2.OpError{Reason: errors2.StatusTimeout}),
		&errors2.TransportError{Reason: errors2.TransportErrorAborted},
	} {
		t.Run("", func(t *testing.T) {
			if errors2.IsOpError(err, code) {
				t.Errorf("expected %v not to be OpError with code=%v", err, code)
			}
		})
	}
}
