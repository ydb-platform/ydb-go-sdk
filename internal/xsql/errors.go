package xsql

import (
	"database/sql/driver"
	"errors"
)

var (
	ErrUnsupported         = driver.ErrSkip
	errDeprecated          = driver.ErrSkip
	errWrongQueryProcessor = errors.New("wrong query processor")
	errNotReadyConn        = errors.New("conn not ready")
)
