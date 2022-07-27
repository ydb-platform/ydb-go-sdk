package xerrors

import (
	"errors"
)

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

func NewYdbErrWithStackTrace(text string) error {
	return WithStackTrace(Wrap(errors.New(text)), WithSkipDepth(1))
}

// Wrap makes internal ydb error
func Wrap(err error) error {
	return &ydbError{err: err}
}
