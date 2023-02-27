//go:build !go1.18
// +build !go1.18

package ydb

import (
	"database/sql"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
)

func Unwrap(db *sql.DB) (*Driver, error) {
	c, err := xsql.Unwrap(db)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	d.connectorsMtx.RLock()
	defer d.connectorsMtx.RUnlock()
	return d.connectors[c], nil
}
