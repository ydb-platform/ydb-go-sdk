package xerrors

import "fmt"

// RetryType reports which operations need to retry
type RetryType uint8

// OperationStatus reports which status of operation: completed or none
type OperationStatus uint8

// Binary flags that used as OperationStatus
const (
	OperationFinished        = OperationStatus(1 << iota >> 1)
	OperationStatusUndefined // may be true or may be false
	OperationNotFinished
)

func (t OperationStatus) String() string {
	switch t {
	case OperationFinished:
		return "operation was completed, no need retries"
	case OperationNotFinished:
		return "operation was not completed, need retries"
	case OperationStatusUndefined:
		return "operation completed status undefined, no need retries"
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
