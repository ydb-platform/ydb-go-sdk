package errors

import (
	"fmt"
	"testing"
)

func TestIsOperationError(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	for _, code := range [...]StatusCode{
		StatusBadRequest,
		StatusBadSession,
	} {
		for _, err := range []error{
			&OpError{Reason: code},
			NewOpError(WithOEReason(code)),
			fmt.Errorf("wrapped: %w", &OpError{Reason: code}),
		} {
			t.Run("", func(t *testing.T) {
				if !IsOpError(err, code) {
					t.Errorf("expected %v to be OpError with code=%v", err, code)
				}
			})
		}
	}
}
