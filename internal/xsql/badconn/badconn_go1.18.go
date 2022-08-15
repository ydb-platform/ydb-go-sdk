//go:build go1.18
// +build go1.18

package badconn

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
)

type badConnError struct {
	err error
}

func (e badConnError) Error() string {
	return "ydbsql: bad connection: " + e.err.Error()
}

func (e badConnError) Unwrap() error {
	return driver.ErrBadConn
}

func (e badConnError) Is(err error) bool {
	return errors.Is(e.err, err)
}

func (e badConnError) As(target interface{}) bool {
	return errors.As(e.err, target)
}

func Map(err error) error {
	if retry.MustDeleteSession(err) {
		return badConnError{err: err}
	}
	return err
}
