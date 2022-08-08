//go:build go1.18
// +build go1.18

package xsql

import (
	"database/sql/driver"
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

func badConn(err error) error {
	return badConnError{err: err}
}
