package xsql

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnsupported         = driver.ErrSkip
	errDeprecated          = driver.ErrSkip
	errAlreadyClosed       = errors.New("already closed")
	errWrongQueryProcessor = errors.New("wrong query processor")
	errNotReadyConn        = xerrors.Retryable(errors.New("iface not ready"), xerrors.InvalidObject())
)
