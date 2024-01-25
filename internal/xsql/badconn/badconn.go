package badconn

import (
	"database/sql/driver"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Error struct {
	err error
}

func (e Error) Origin() error {
	return e.err
}

func (e Error) Error() string {
	return e.err.Error()
}

func (e Error) Is(err error) bool {
	//nolint:nolintlint
	if err == driver.ErrBadConn { //nolint:errorlint
		return true
	}
	return xerrors.Is(e.err, err)
}

func (e Error) As(target interface{}) bool {
	switch target.(type) {
	case Error, *Error:
		return true
	default:
		return xerrors.As(e.err, target)
	}
}

func Map(err error) error {
	switch {
	case err == nil:
		return nil
	case xerrors.Is(err, io.EOF):
		return io.EOF
	case xerrors.MustDeleteSession(err):
		return Error{err: err}
	default:
		return err
	}
}
