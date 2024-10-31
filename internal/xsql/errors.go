package xsql

import (
	"database/sql/driver"
	"errors"
)

var (
	ErrUnsupported         = driver.ErrSkip
	errDeprecated          = driver.ErrSkip
	errAlreadyClosed       = errors.New("already closed")
	errWrongQueryProcessor = errors.New("wrong query processor")
)
