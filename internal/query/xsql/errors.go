package xsql

import (
	"database/sql/driver"
)

var (
	ErrUnsupported = driver.ErrSkip
)
