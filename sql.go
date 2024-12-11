package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	connOverQueryServiceClient "github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/conn/query"
	connOverTableServiceClient "github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/conn/table"
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
	connectors xsync.Map[*xsql.Connector, *Driver]
}

var (
	_ driver.Driver        = &sqlDriver{}
	_ driver.DriverContext = &sqlDriver{}
)

func (d *sqlDriver) Close() error {
	var errs []error
	d.connectors.Range(func(c *xsql.Connector, _ *Driver) bool {
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
	d.connectors.Set(c, parent)
}

func (d *sqlDriver) detach(c *xsql.Connector) {
	d.connectors.Delete(c)
}

type QueryMode int

const (
	_ = QueryMode(iota)
	DataQueryMode
	ExplainQueryMode
	ScanQueryMode
	SchemeQueryMode
	ScriptingQueryMode
	QueryExecuteQueryMode
)

// WithQueryMode set query mode for legacy database/sql driver
//
// For actual database/sql driver works over query service client and no needs query mode
func WithQueryMode(ctx context.Context, mode QueryMode) context.Context {
	switch mode {
	case ExplainQueryMode:
		return xsql.WithExplain(ctx)
	case DataQueryMode:
		return connOverTableServiceClient.WithQueryMode(ctx, connOverTableServiceClient.DataQueryMode)
	case ScanQueryMode:
		return connOverTableServiceClient.WithQueryMode(ctx, connOverTableServiceClient.ScanQueryMode)
	case SchemeQueryMode:
		return connOverTableServiceClient.WithQueryMode(ctx, connOverTableServiceClient.SchemeQueryMode)
	case ScriptingQueryMode:
		return connOverTableServiceClient.WithQueryMode(ctx, connOverTableServiceClient.ScriptingQueryMode)
	default:
		return ctx
	}
}

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return connOverTableServiceClient.WithTxControl(ctx, txc)
}

type ConnectorOption = xsql.Option

type QueryBindConnectorOption interface {
	ConnectorOption
	bind.Bind
}

func modeToMode(mode QueryMode) connOverTableServiceClient.QueryMode {
	switch mode {
	case ScanQueryMode:
		return connOverTableServiceClient.ScanQueryMode
	case SchemeQueryMode:
		return connOverTableServiceClient.SchemeQueryMode
	case ScriptingQueryMode:
		return connOverTableServiceClient.ScriptingQueryMode
	default:
		return connOverTableServiceClient.DataQueryMode
	}
}

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return xsql.WithTableOptions(
		connOverTableServiceClient.WithDefaultQueryMode(modeToMode(mode)),
	)
}

func WithFakeTx(modes ...QueryMode) ConnectorOption {
	opts := make([]ConnectorOption, 0, len(modes))

	for _, mode := range modes {
		switch mode {
		case DataQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(connOverTableServiceClient.WithFakeTxModes(
					connOverTableServiceClient.DataQueryMode,
				)),
			)
		case ScanQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(connOverTableServiceClient.WithFakeTxModes(
					connOverTableServiceClient.ScanQueryMode,
				)),
			)
		case SchemeQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(connOverTableServiceClient.WithFakeTxModes(
					connOverTableServiceClient.SchemeQueryMode,
				)),
			)
		case ScriptingQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(connOverTableServiceClient.WithFakeTxModes(
					connOverTableServiceClient.DataQueryMode,
				)),
			)
		case QueryExecuteQueryMode:
			opts = append(opts,
				xsql.WithQueryOptions(connOverQueryServiceClient.WithFakeTx()),
			)
		default:
		}
	}

	return xsql.Merge(opts...)
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
	return xsql.WithTableOptions(connOverTableServiceClient.WithDefaultTxControl(txControl))
}

func WithDefaultDataQueryOptions(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return xsql.WithTableOptions(connOverTableServiceClient.WithDataOpts(opts...))
}

func WithDefaultScanQueryOptions(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return xsql.WithTableOptions(connOverTableServiceClient.WithScanOpts(opts...))
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
	c, err := xsql.Open(parent, parent.metaBalancer,
		append(
			append(
				parent.databaseSQLOptions,
				opts...,
			),
			xsql.WithOnClose(d.detach),
			xsql.WithTraceRetry(parent.config.TraceRetry()),
			xsql.WithRetryBudget(parent.config.RetryBudget()),
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
