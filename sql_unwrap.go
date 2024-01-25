package ydb

import (
	"database/sql"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
)

func Unwrap[T *sql.DB | *sql.Conn](v T) (*Driver, error) {
	c, err := xsql.Unwrap(v)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	d.connectorsMtx.RLock()
	defer d.connectorsMtx.RUnlock()

	return d.connectors[c], nil
}
