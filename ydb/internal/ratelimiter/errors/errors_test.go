package errors

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestIsAcquireError(t *testing.T) {
	for _, err := range []error{
		&acquireError{},
		xerrors.WithStackTrace(&acquireError{}),
		xerrors.WithStackTrace(fmt.Errorf("%w", &acquireError{})),
	} {
		t.Run("", func(t *testing.T) {
			if !IsAcquireError(err) {
				t.Errorf("not acquire error: %v", err)
			}
		})
	}
}

func TestToAcquireError(t *testing.T) {
	for _, err := range []error{
		&acquireError{},
		xerrors.WithStackTrace(&acquireError{}),
		xerrors.WithStackTrace(fmt.Errorf("%w", &acquireError{})),
	} {
		t.Run("", func(t *testing.T) {
			ae := ToAcquireError(err)
			if ae == nil {
				t.Errorf("not acquire error: %v", err)
			}
		})
	}
}
