package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/connector"
	tableSql "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var d = &sqlDriver{} //nolint:gochecknoglobals

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
	connectors xsync.Map[*connector.Connector, *Driver]
}

var (
	_ driver.Driver        = &sqlDriver{}
	_ driver.DriverContext = &sqlDriver{}
)

func (d *sqlDriver) Close() error {
	var errs []error
	d.connectors.Range(func(c *connector.Connector, _ *Driver) bool {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}

		return true
	})
	if len(errs) > 0 {
		return xerrors.NewWithIssues("ydb legacy driver close failed", errs...)
	}

	return nil
}

// Open returns a new Driver to the ydb.
func (d *sqlDriver) Open(string) (driver.Conn, error) {
	return nil, connector.ErrUnsupported
}

func (d *sqlDriver) OpenConnector(dataSourceName string) (driver.Connector, error) {
	db, err := Open(context.Background(), dataSourceName)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("failed to connect by data source name '%s': %w", dataSourceName, err))
	}

	return Connector(db, db.databaseSQLOptions...)
}

func (d *sqlDriver) attach(c *connector.Connector, parent *Driver) {
	d.connectors.Set(c, parent)
}

func (d *sqlDriver) detach(c *connector.Connector) {
	d.connectors.Delete(c)
}

type QueryMode = tableSql.QueryMode

const (
	DataQueryMode      = tableSql.DataQueryMode
	ExplainQueryMode   = tableSql.ExplainQueryMode
	ScanQueryMode      = tableSql.ScanQueryMode
	SchemeQueryMode    = tableSql.SchemeQueryMode
	ScriptingQueryMode = tableSql.ScriptingQueryMode
)

func WithQueryMode(ctx context.Context, mode QueryMode) context.Context {
	return xcontext.WithQueryMode(ctx, mode)
}

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return tableSql.WithTxControl(ctx, txc)
}

type ConnectorOption = connector.Option

type QueryBindConnectorOption interface {
	ConnectorOption
	bind.Bind
}

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return connector.WithTableOptions(tableSql.WithDefaultQueryMode(mode))
}

func WithFakeTx(mode QueryMode) ConnectorOption {
	return connector.WithTableOptions(tableSql.WithFakeTxModes(mode))
}

func WithTablePathPrefix(tablePathPrefix string) QueryBindConnectorOption {
	return connector.WithTablePathPrefix(tablePathPrefix)
}

func WithAutoDeclare() QueryBindConnectorOption {
	return connector.WithQueryBind(bind.AutoDeclare{})
}

func WithPositionalArgs() QueryBindConnectorOption {
	return connector.WithQueryBind(bind.PositionalArgs{})
}

func WithNumericArgs() QueryBindConnectorOption {
	return connector.WithQueryBind(bind.NumericArgs{})
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return connector.WithTableOptions(tableSql.WithDefaultTxControl(txControl))
}

func WithDefaultDataQueryOptions(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return connector.WithTableOptions(tableSql.WithDataOpts(opts...))
}

func WithDefaultScanQueryOptions(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return connector.WithTableOptions(tableSql.WithScanOpts(opts...))
}

func WithDatabaseSQLTrace(
	t trace.DatabaseSQL, //nolint:gocritic
	opts ...trace.DatabaseSQLComposeOption,
) ConnectorOption {
	return connector.WithTrace(&t, opts...)
}

func WithDisableServerBalancer() ConnectorOption {
	return connector.WithDisableServerBalancer()
}

type SQLConnector interface {
	driver.Connector

	Close() error
}

func Connector(parent *Driver, opts ...ConnectorOption) (SQLConnector, error) {
	c, err := connector.Open(parent, parent.metaBalancer,
		append(
			append(
				parent.databaseSQLOptions,
				opts...,
			),
			connector.WithOnClose(d.detach),
			connector.WithTraceRetry(parent.config.TraceRetry()),
			connector.WithRetryBudget(parent.config.RetryBudget()),
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
