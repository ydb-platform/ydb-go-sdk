//go:build go1.18
// +build go1.18

package badconn

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type badConnError struct {
	err error
}

func (e badConnError) Error() string {
	return e.err.Error()
}

func (e badConnError) Is(err error) bool {
	//nolint:errorlint
	if err == driver.ErrBadConn {
		return true
	}
	return xerrors.Is(e.err, err)
}

func (e badConnError) As(target interface{}) bool {
	return xerrors.As(e.err, target)
}

func Map(err error) error {
	if retry.MustDeleteSession(err) {
		return badConnError{err: err}
	}
	return err
}
