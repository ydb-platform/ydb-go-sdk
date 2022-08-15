package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var d = &sqlDriver{connectors: make(map[*xsql.Connector]struct{})}

func init() {
	sql.Register("ydb", d)
	sql.Register("ydb/v3", d)
}

// Driver is an adapter to allow the use table client as conn.Driver instance.
type sqlDriver struct {
	connectors    map[*xsql.Connector]struct{}
	connectorsMtx xsync.RWMutex
}

var (
	_ driver.Driver        = &sqlDriver{}
	_ driver.DriverContext = &sqlDriver{}
)

func (d *sqlDriver) Close() error {
	var connectors map[*xsql.Connector]struct{}
	d.connectorsMtx.WithRLock(func() {
		connectors = d.connectors
	})
	var errs []error
	for c := range connectors {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return xerrors.NewWithIssues("ydb legacy driver close failed", errs...)
	}
	return nil
}

// Open returns a new connection to the ydb.
func (d *sqlDriver) Open(string) (driver.Conn, error) {
	return nil, xsql.ErrUnsupported
}

func (d *sqlDriver) OpenConnector(dataSourceName string) (driver.Connector, error) {
	opts, connectorOpts, err := xsql.Parse(dataSourceName)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	db, err := Open(context.Background(), dataSourceName, With(opts...))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	c, err := xsql.Open(d, db, connectorOpts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}

func (d *sqlDriver) Attach(c *xsql.Connector) {
	d.connectorsMtx.WithLock(func() {
		d.connectors[c] = struct{}{}
	})
}

func (d *sqlDriver) Detach(c *xsql.Connector) {
	d.connectorsMtx.WithLock(func() {
		delete(d.connectors, c)
	})
}

type QueryMode = xsql.QueryMode

const (
	DataQueryMode      = xsql.DataQueryMode
	ExplainQueryMode   = xsql.ExplainQueryMode
	ScanQueryMode      = xsql.ScanQueryMode
	SchemeQueryMode    = xsql.SchemeQueryMode
	ScriptingQueryMode = xsql.ScriptingQueryMode
)

func WithQueryMode(ctx context.Context, mode QueryMode) context.Context {
	return xsql.WithQueryMode(ctx, mode)
}

type ConnectorOption = xsql.ConnectorOption

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return xsql.WithDefaultQueryMode(mode)
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return xsql.WithDefaultTxControl(txControl)
}

func WithDefaultDataQueryOptions(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return xsql.WithDefaultDataQueryOptions(opts...)
}

func WithDefaultScanQueryOptions(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return xsql.WithDefaultScanQueryOptions(opts...)
}

func WithDatabaseSQLTrace(t trace.DatabaseSQL, opts ...trace.DatabaseSQLComposeOption) ConnectorOption {
	return xsql.WithTrace(t, opts...)
}

func Connector(db Connection, opts ...ConnectorOption) (*xsql.Connector, error) {
	if c, ok := db.(*connection); ok {
		opts = append(opts, c.sqlOptions...)
	}
	c, err := xsql.Open(d, db, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}

func Unwrap(db *sql.DB) (Connection, error) {
	c, err := xsql.Unwrap(db)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if cc, ok := c.Connection().(Connection); ok {
		return cc, nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("%+v is not a ydb.Nonnection", c.Connection()))
}
