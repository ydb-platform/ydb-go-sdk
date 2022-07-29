//go:build go1.18
// +build go1.18

package xsql

import (
	"database/sql/driver"

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

func BadConn(err error) error {
	if err == nil {
		return nil
	}
	_, _, _, deleteSession := retry.Check(err)
	if deleteSession {
		return badConn(err)
	}
	return err
}

func badConn(err error) error {
	return badConnError{err: err}
}
