//go:build go1.18
// +build go1.18

package ydb

import (
	"database/sql"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
)

func Unwrap[T *sql.DB | *sql.Conn](v T) (*Connection, error) {
	c, err := xsql.Unwrap(v)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if cc, ok := c.Connection().(*Connection); ok {
		return cc, nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("%+v is not a ydb.Connection", c.Connection()))
}
