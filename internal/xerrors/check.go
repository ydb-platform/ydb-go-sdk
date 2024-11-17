package xerrors

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

// Check returns retry mode for err.
func Check(err error) (
	code int64,
	errType Type,
	backoffType backoff.Type,
	invalidObject bool,
) {
	fmt.Println("CHECK 1")
	if err == nil {
		fmt.Println("CHECK 2")
		return -1,
			TypeNoError,
			backoff.TypeNoBackoff,
			false
	}
	var e Error
	if As(err, &e) {
		fmt.Println("CHECK 3", fmt.Sprintf("%T", e), e.Code(), e.Name(), e.Type(), e.BackoffType(), e.IsRetryObjectValid())
		return int64(e.Code()), e.Type(), e.BackoffType(), !e.IsRetryObjectValid()
	}

	fmt.Println("CHECK 4")
	return -1,
		TypeNonRetryable, // unknown errors are not retryable
		backoff.TypeNoBackoff,
		false
}

func IsRetryObjectValid(err error) bool {
	if err == nil {
		return true
	}

	var e Error
	if As(err, &e) {
		return e.IsRetryObjectValid()
	}

	return true
}
