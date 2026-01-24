package badconn

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Error struct {
	xerrors.ErrorWithCastToTargetError
}

func (e Error) Error() string {
	return e.Err.Error()
}

func (e Error) As(target interface{}) bool {
	switch target.(type) {
	case Error, *Error:
		return true
	default:
		return xerrors.As(e.Err, target)
	}
}

func New(msg string) error {
	return &Error{
		ErrorWithCastToTargetError: xerrors.ErrorWithCastToTargetError{
			Err:    errors.New(msg),
			Target: driver.ErrBadConn,
		},
	}
}

func Errorf(format string, args ...interface{}) error {
	return &Error{
		ErrorWithCastToTargetError: xerrors.ErrorWithCastToTargetError{
			Err:    fmt.Errorf(format, args...),
			Target: driver.ErrBadConn,
		},
	}
}

func Map(err error) error {
	switch {
	case err == nil:
		return nil
	case xerrors.Is(err, io.EOF):
		return io.EOF
	case xerrors.MustDeleteTableOrQuerySession(err):
		return &Error{
			ErrorWithCastToTargetError: xerrors.ErrorWithCastToTargetError{
				Err:    err,
				Target: driver.ErrBadConn,
			},
		}
	default:
		return err
	}
}
