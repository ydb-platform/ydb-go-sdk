package xsql

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnsupported     = driver.ErrSkip
	errDeprecated      = driver.ErrSkip
	errConnClosedEarly = xerrors.Retryable(errors.New("conn closed early"), xerrors.InvalidObject())
	errNotReadyConn    = xerrors.Retryable(errors.New("conn not ready"), xerrors.InvalidObject())
)

type ConnAlreadyHaveTxError struct {
	currentTx string
}

func (err *ConnAlreadyHaveTxError) Error() string {
	return "conn already have an open currentTx: " + err.currentTx
}

func (err *ConnAlreadyHaveTxError) As(target interface{}) bool {
	switch t := target.(type) {
	case *ConnAlreadyHaveTxError:
		t.currentTx = err.currentTx

		return true
	default:
		return false
	}
}
