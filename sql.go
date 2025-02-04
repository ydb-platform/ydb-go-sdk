package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/legacy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/propose"
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

type sqlDriver struct{}

var (
	_ driver.Driver        = &sqlDriver{}
	_ driver.DriverContext = &sqlDriver{}
)

// Open returns a new Driver to the ydb.
func (d *sqlDriver) Open(string) (driver.Conn, error) {
	return nil, xsql.ErrUnsupported
}

func (d *sqlDriver) OpenConnector(dataSourceName string) (driver.Connector, error) {
	db, err := Open(context.Background(), dataSourceName)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("failed to connect by data source name '%s': %w", dataSourceName, err))
	}

	c, err := Connector(db, append(db.databaseSQLOptions,
		xsql.WithOnClose(func(connector *xsql.Connector) {
			_ = db.Close(context.Background())
		}),
	)...)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("failed to create connector: %w", err))
	}

	return c, nil
}

type QueryMode int

const (
	unknownQueryMode = QueryMode(iota)
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
		return legacy.WithQueryMode(ctx, legacy.DataQueryMode)
	case ScanQueryMode:
		return legacy.WithQueryMode(ctx, legacy.ScanQueryMode)
	case SchemeQueryMode:
		return legacy.WithQueryMode(ctx, legacy.SchemeQueryMode)
	case ScriptingQueryMode:
		return legacy.WithQueryMode(ctx, legacy.ScriptingQueryMode)
	default:
		return ctx
	}
}

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return legacy.WithTxControl(ctx, txc)
}

type ConnectorOption = xsql.Option

type QueryBindConnectorOption interface {
	ConnectorOption
	bind.Bind
}

func modeToMode(mode QueryMode) legacy.QueryMode {
	switch mode {
	case ScanQueryMode:
		return legacy.ScanQueryMode
	case SchemeQueryMode:
		return legacy.SchemeQueryMode
	case ScriptingQueryMode:
		return legacy.ScriptingQueryMode
	default:
		return legacy.DataQueryMode
	}
}

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return xsql.WithTableOptions(
		legacy.WithDefaultQueryMode(modeToMode(mode)),
	)
}

// WithQueryService is an experimental flag for create database/sql driver over query service client
//
// By default database/sql driver works over table service client
// Default will be changed to `WithQueryService` after March 2025
func WithQueryService(b bool) ConnectorOption {
	return xsql.WithQueryService(b)
}

func WithFakeTx(modes ...QueryMode) ConnectorOption {
	opts := make([]ConnectorOption, 0, len(modes))

	for _, mode := range modes {
		switch mode {
		case DataQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(legacy.WithFakeTxModes(
					legacy.DataQueryMode,
				)),
			)
		case ScanQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(legacy.WithFakeTxModes(
					legacy.ScanQueryMode,
				)),
			)
		case SchemeQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(legacy.WithFakeTxModes(
					legacy.SchemeQueryMode,
				)),
			)
		case ScriptingQueryMode:
			opts = append(opts,
				xsql.WithTableOptions(legacy.WithFakeTxModes(
					legacy.ScriptingQueryMode,
				)),
				xsql.WithQueryOptions(propose.WithFakeTx()),
			)
		case QueryExecuteQueryMode:
			opts = append(opts,
				xsql.WithQueryOptions(propose.WithFakeTx()),
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
	return xsql.WithTableOptions(legacy.WithDefaultTxControl(txControl))
}

func WithDefaultDataQueryOptions(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return xsql.WithTableOptions(legacy.WithDataOpts(opts...))
}

func WithDefaultScanQueryOptions(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return xsql.WithTableOptions(legacy.WithScanOpts(opts...))
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
			xsql.WithTraceRetry(parent.config.TraceRetry()),
			xsql.WithRetryBudget(parent.config.RetryBudget()),
		)...,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return c, nil
}

func MustConnector(parent *Driver, opts ...ConnectorOption) SQLConnector {
	c, err := Connector(parent, opts...)
	if err != nil {
		panic(err)
	}

	return c
}
