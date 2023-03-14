//go:build !go1.18
// +build !go1.18

package badconn

import (
	"database/sql/driver"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func Map(err error) error {
	switch {
	case err == nil:
		return nil
	case xerrors.Is(err, io.EOF):
		return io.EOF
	case xerrors.Is(err, driver.ErrBadConn), xerrors.MustDeleteSession(err):
		return driver.ErrBadConn
	default:
		return err
	}
}
