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

type ErrConnAlreadyHaveTx struct {
	currentTx string
}

func (err *ErrConnAlreadyHaveTx) Error() string {
	return "conn already have an open currentTx: " + err.currentTx
}

func (err *ErrConnAlreadyHaveTx) As(target interface{}) bool {
	switch t := target.(type) {
	case *ErrConnAlreadyHaveTx:
		t.currentTx = err.currentTx
		return true
	default:
		return false
	}
}
