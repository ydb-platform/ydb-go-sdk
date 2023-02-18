package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"

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
	return Connector(db, connectorOpts...)
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

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return xsql.WithTxControl(ctx, txc)
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

func WithDisableServerBalancer() ConnectorOption {
	return xsql.WithDisableServerBalancer()
}

func Connector(db *Connection, opts ...ConnectorOption) (*xsql.Connector, error) {
	c, err := xsql.Open(d, db, append(db.databaseSQLOptions, opts...)...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}
