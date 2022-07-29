//go:build !go1.18
// +build !go1.18

package xsql

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
)

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
	return driver.ErrBadConn
}
