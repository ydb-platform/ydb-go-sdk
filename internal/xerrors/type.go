package xerrors

import "fmt"

// Type reports which error type
type Type uint8

const (
	TypeNoError = Type(iota)
	TypeNonRetryable
	TypeConditionallyRetryable
	TypeRetryable
)

func (t Type) String() string {
	switch t {
	case TypeNoError:
		return "no error"
	case TypeNonRetryable:
		return "non-retryable"
	case TypeRetryable:
		return "retryable"
	case TypeConditionallyRetryable:
		return "conditionally retryable"
	default:
		return fmt.Sprintf("unknown error type %d", t)
	}
}
