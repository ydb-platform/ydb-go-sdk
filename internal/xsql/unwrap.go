package xsql

import (
	"database/sql"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
)

func Unwrap[T *sql.DB | *sql.Conn](v T) (connector *Connector, err error) {
	switch vv := any(v).(type) {
	case *sql.DB:
		d := vv.Driver()
		if dw, ok := d.(*driverWrapper); ok {
			return dw.c, nil
		}
		return nil, xerrors.WithStackTrace(fmt.Errorf("%T is not a *driverWrapper", d))
	case *sql.Conn:
		if err = vv.Raw(func(driverConn interface{}) error {
			if cc, ok := driverConn.(*conn); ok {
				connector = cc.connector
				return nil
			}
			return xerrors.WithStackTrace(fmt.Errorf("%T is not a *conn", driverConn))
		}); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return connector, nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("unknown type %T for Unwrap", vv))
	}
}
