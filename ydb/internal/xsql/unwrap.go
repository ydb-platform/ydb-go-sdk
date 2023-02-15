//go:build !go1.18
// +build !go1.18

package xsql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
)

func Unwrap(db *sql.DB) (connector *Connector, err error) {
	// hop with create session (connector.Connect()) helps to get ydb.Connection
	c, err := db.Conn(context.Background())
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	if err = c.Raw(func(driverConn interface{}) error {
		if cc, ok := driverConn.(*conn); ok {
			connector = cc.connector
			return nil
		}
		return xerrors.WithStackTrace(badconn.Map(fmt.Errorf("%+v is not a *conn", driverConn)))
	}); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	return connector, nil
}
