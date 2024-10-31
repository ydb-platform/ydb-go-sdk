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

type AlreadyHaveTxError struct {
	currentTx string
}

func (err *AlreadyHaveTxError) Error() string {
	return "Conn already have an open currentTx: " + err.currentTx
}

func (err *AlreadyHaveTxError) As(target interface{}) bool {
	switch t := target.(type) {
	case *AlreadyHaveTxError:
		t.currentTx = err.currentTx

		return true
	default:
		return false
	}
}
