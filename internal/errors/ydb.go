package errors

import "errors"

type isYdbError interface {
	isYdbError()
}

func IsYdb(err error) bool {
	var e isYdbError
	return errors.As(err, &e)
}

type ydbError struct {
	err error
}

func (e *ydbError) isYdbError() {}

func (e *ydbError) Error() string {
	return e.err.Error()
}

// New makes internal ydb error
func New(err error) error {
	return &ydbError{err: err}
}
