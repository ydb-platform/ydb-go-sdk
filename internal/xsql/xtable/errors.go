package xtable

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnsupported     = driver.ErrSkip
	errConnClosedEarly = xerrors.Retryable(errors.New("conn closed early"), xerrors.InvalidObject())
	errNotReadyConn    = xerrors.Retryable(errors.New("conn not ready"), xerrors.InvalidObject())
	ErrWrongQueryMode  = errors.New("wrong query mode")
)
