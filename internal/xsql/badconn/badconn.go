//go:build !go1.18
// +build !go1.18

package badconn

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
)

func Map(err error) error {
	if retry.MustDeleteSession(err) {
		return driver.ErrBadConn
	}
	return err
}
