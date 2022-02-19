package ratelimiter

import "fmt"

type acquireError struct {
	err    error
	amount uint64
}

func (e *acquireError) Error() string {
	return fmt.Sprintf("acquire for amount (%d) failed: %v", e.amount, e.err)
}

func (e *acquireError) Unwrap() error {
	return e.err
}

func AcquireError(amount uint64, err error) error {
	return &acquireError{
		err:    err,
		amount: amount,
	}
}
