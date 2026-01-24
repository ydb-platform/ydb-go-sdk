package badconn

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Error struct {
	error
}

func (e Error) Unwrap() error {
	return e.error
}

func New(msg string) error {
	return &Error{xerrors.IsTarget(errors.New(msg), driver.ErrBadConn)}
}

func Errorf(format string, args ...interface{}) error {
	return &Error{xerrors.IsTarget(fmt.Errorf(format, args...), driver.ErrBadConn)}
}

func Map(err error) error {
	switch {
	case err == nil:
		return nil
	case xerrors.Is(err, io.EOF):
		return io.EOF
	case xerrors.MustDeleteTableOrQuerySession(err):
		return &Error{xerrors.IsTarget(err, driver.ErrBadConn)}
	default:
		return err
	}
}
