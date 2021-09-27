package errors

import "fmt"

// RetryType reports which operations need to retry
type RetryType uint8

// OperationCompleted reports which status of operation: completed or none
type OperationCompleted uint8

// Binary flags that used as OperationCompleted
const (
	OperationCompletedTrue      OperationCompleted = 1 << iota >> 1
	OperationCompletedUndefined                    // may be true or may be false
	OperationCompletedFalse
)

func (t OperationCompleted) String() string {
	switch t {
	case OperationCompletedTrue:
		return "operation was completed"
	case OperationCompletedFalse:
		return "operation was not completed"
	case OperationCompletedUndefined:
		return "operation completed status undefined"
	default:
		return fmt.Sprintf("unknown operation completed code: %d", t)
	}
}

// BackoffType reports how to Backoff operation
type BackoffType uint8

// Binary flags that used as BackoffType
const (
	BackoffTypeNoBackoff BackoffType = 1 << iota >> 1

	BackoffTypeFastBackoff
	BackoffTypeSlowBackoff

	BackoffTypeBackoffAny = BackoffTypeFastBackoff | BackoffTypeSlowBackoff
)

func (b BackoffType) String() string {
	switch b {
	case BackoffTypeNoBackoff:
		return "immediately"
	case BackoffTypeFastBackoff:
		return "fast backoff"
	case BackoffTypeSlowBackoff:
		return "slow backoff"
	case BackoffTypeBackoffAny:
		return "any backoff"
	default:
		return fmt.Sprintf("unknown backoff type %d", b)
	}
}
