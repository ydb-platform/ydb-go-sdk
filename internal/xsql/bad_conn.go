//go:build !go1.18
// +build !go1.18

package xsql

import (
	"database/sql/driver"
)

func badConn(err error) error {
	return driver.ErrBadConn
}
