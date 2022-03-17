package errors

import (
	"fmt"
	"testing"
)

func TestIsOperationError(t *testing.T) {
	for _, code := range [...]StatusCode{
		StatusBadRequest,
		StatusBadSession,
	} {
		for _, err := range []error{
			&OperationError{Reason: code},
			NewOpError(WithOEReason(code)),
			fmt.Errorf("wrapped: %w", &OperationError{Reason: code}),
		} {
			t.Run("", func(t *testing.T) {
				if !IsOpError(err, code) {
					t.Errorf("expected %v to be OperationError with code=%v", err, code)
				}
			})
		}
	}
}
