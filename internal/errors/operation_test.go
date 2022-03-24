package errors

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestIsOperationError(t *testing.T) {
	for _, code := range [...]Ydb.StatusIds_StatusCode{
		Ydb.StatusIds_BAD_REQUEST,
		Ydb.StatusIds_BAD_SESSION,
	} {
		for _, err := range []error{
			&operationError{code: code},
			Operation(WithStatusCode(code)),
			fmt.Errorf("wrapped: %w", &operationError{code: code}),
		} {
			t.Run("", func(t *testing.T) {
				if !IsOperationError(err, code) {
					t.Errorf("expected %v to be operationError with code=%v", err, code)
				}
			})
		}
	}
}
