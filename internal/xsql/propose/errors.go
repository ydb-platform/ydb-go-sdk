package propose

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnsupported     = driver.ErrSkip
	errDeprecated      = driver.ErrSkip
	errConnClosedEarly = xerrors.Retryable(errors.New("iface closed early"), xerrors.InvalidObject())
	errNotReadyConn    = xerrors.Retryable(errors.New("iface not ready"), xerrors.InvalidObject())
)
