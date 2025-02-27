package xtable

import (
	"database/sql/driver"
	"errors"
)

var (
	ErrUnsupported     = driver.ErrSkip
	errConnClosedEarly = errors.New("conn closed early")
	errNotReadyConn    = errors.New("conn not ready")
	ErrWrongQueryMode  = errors.New("wrong query mode")
)
