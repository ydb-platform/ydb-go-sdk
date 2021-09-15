package errors

// RetryType reports which operations need to retry
type RetryType uint8

// Binary flags that used as RetryType
const (
	RetryTypeNoRetry RetryType = 1 << iota >> 1
	RetryTypeIdempotent
	RetryTypeNoIdempotent

	RetryTypeAny = RetryTypeNoIdempotent | RetryTypeIdempotent
)

// BackoffType reports how to Backoff operation
type BackoffType uint8

// Binary flags that used as BackoffType
const (
	BackoffTypeNoBackoff BackoffType = 1 << iota >> 1

	BackoffTypeFastBackoff
	BackoffTypeSlowBackoff

	BackoffTypeBackoffAny = BackoffTypeFastBackoff | BackoffTypeSlowBackoff
)
