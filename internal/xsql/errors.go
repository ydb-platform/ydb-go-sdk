package xsql

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
)

var (
	ErrUnsupported  = driver.ErrSkip
	errDeprecated   = driver.ErrSkip
	errClosedConn   = badconn.Map(xerrors.Retryable(errors.New("conn closed early"), xerrors.WithDeleteSession()))
	errNotReadyConn = badconn.Map(xerrors.Retryable(errors.New("conn not ready"), xerrors.WithDeleteSession()))
)
