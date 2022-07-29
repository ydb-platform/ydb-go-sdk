package xsql

import (
	"database/sql/driver"
	"errors"
)

var (
	ErrUnsupported = driver.ErrSkip
	errDeprecated  = driver.ErrSkip
	errClosedConn  = badConn(errors.New("closed conn"))
)
