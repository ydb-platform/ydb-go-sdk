package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var d = &sqlDriver{connectors: make(map[*xsql.Connector]*Driver)} //nolint:gochecknoglobals

func init() { //nolint:gochecknoinits
	sql.Register("ydb", d)
	sql.Register("ydb/v3", d)
}

func withConnectorOptions(opts ...ConnectorOption) Option {
	return func(ctx context.Context, d *Driver) error {
		d.databaseSQLOptions = append(d.databaseSQLOptions, opts...)

		return nil
	}
}

type sqlDriver struct {
	connectors    map[*xsql.Connector]*Driver
	connectorsMtx xsync.RWMutex
}

var (
	_ driver.Driver        = &sqlDriver{}
	_ driver.DriverContext = &sqlDriver{}
)

func (d *sqlDriver) Close() error {
	var connectors map[*xsql.Connector]*Driver
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

// Open returns a new Driver to the ydb.
func (d *sqlDriver) Open(string) (driver.Conn, error) {
	return nil, xsql.ErrUnsupported
}

func (d *sqlDriver) OpenConnector(dataSourceName string) (driver.Connector, error) {
	db, err := Open(context.Background(), dataSourceName)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("failed to connect by data source name '%s': %w", dataSourceName, err))
	}

	return Connector(db, db.databaseSQLOptions...)
}

func (d *sqlDriver) attach(c *xsql.Connector, parent *Driver) {
	d.connectorsMtx.WithLock(func() {
		d.connectors[c] = parent
	})
}

func (d *sqlDriver) detach(c *xsql.Connector) {
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

type QueryBindConnectorOption interface {
	ConnectorOption
	bind.Bind
}

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return xsql.WithDefaultQueryMode(mode)
}

func WithFakeTx(mode QueryMode) ConnectorOption {
	return xsql.WithFakeTx(mode)
}

func WithTablePathPrefix(tablePathPrefix string) QueryBindConnectorOption {
	return xsql.WithTablePathPrefix(tablePathPrefix)
}

func WithAutoDeclare() QueryBindConnectorOption {
	return xsql.WithQueryBind(bind.AutoDeclare{})
}

func WithPositionalArgs() QueryBindConnectorOption {
	return xsql.WithQueryBind(bind.PositionalArgs{})
}

func WithNumericArgs() QueryBindConnectorOption {
	return xsql.WithQueryBind(bind.NumericArgs{})
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

func WithDatabaseSQLTrace(
	t trace.DatabaseSQL, //nolint:gocritic
	opts ...trace.DatabaseSQLComposeOption,
) ConnectorOption {
	return xsql.WithTrace(&t, opts...)
}

func WithDisableServerBalancer() ConnectorOption {
	return xsql.WithDisableServerBalancer()
}

type SQLConnector interface {
	driver.Connector

	Close() error
}

func Connector(parent *Driver, opts ...ConnectorOption) (SQLConnector, error) {
	c, err := xsql.Open(parent,
		append(
			append(
				parent.databaseSQLOptions,
				opts...,
			),
			xsql.WithOnClose(d.detach),
			xsql.WithTraceRetry(parent.config.TraceRetry()),
			xsql.WithretryBudget(parent.config.RetryBudget()),
		)...,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	d.attach(c, parent)

	return c, nil
}

func MustConnector(parent *Driver, opts ...ConnectorOption) SQLConnector {
	c, err := Connector(parent, opts...)
	if err != nil {
		panic(err)
	}

	return c
}
