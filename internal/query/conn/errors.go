package conn

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnsupported     = driver.ErrSkip
	errDeprecated      = driver.ErrSkip
	errConnClosedEarly = xerrors.Retryable(errors.New("Conn closed early"), xerrors.InvalidObject())
	errNotReadyConn    = xerrors.Retryable(errors.New("Conn not ready"), xerrors.InvalidObject())
)
