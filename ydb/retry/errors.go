//go:build !go1.18
// +build !go1.18

package retry

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func unwrapErrBadConn(err error) error {
	if xerrors.Is(err, driver.ErrBadConn) {
		return xerrors.Retryable(err, xerrors.WithDeleteSession())
	}
	return err
}
