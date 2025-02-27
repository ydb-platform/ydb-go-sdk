package xquery

import (
	"database/sql/driver"
	"errors"
)

var (
	ErrUnsupported     = driver.ErrSkip
	errDeprecated      = driver.ErrSkip
	errConnClosedEarly = errors.New("conn closed early")
	errNotReadyConn    = errors.New("conn not ready")
)
