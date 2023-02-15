package xsql

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnsupported     = driver.ErrSkip
	errDeprecated      = driver.ErrSkip
	errConnClosedEarly = xerrors.Retryable(errors.New("conn closed early"), xerrors.WithDeleteSession())
	errNotReadyConn    = xerrors.Retryable(errors.New("conn not ready"), xerrors.WithDeleteSession())
)
