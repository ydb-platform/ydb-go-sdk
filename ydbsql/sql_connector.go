package ydbsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type sqlOption func(*sqlConnector)

func With(options ...ydb.Option) sqlOption {
	return func(c *sqlConnector) {
		c.options = append(c.options, options...)
	}
}

func WithDefaultTxControl(txControl *table.TransactionControl) sqlOption {
	return func(c *sqlConnector) {
		c.defaultTxControl = txControl
	}
}

func WithDefaultExecDataQueryOption(opts ...options.ExecuteDataQueryOption) sqlOption {
	return func(c *sqlConnector) {
		c.dataOpts = append(c.dataOpts, opts...)
	}
}

func WithDefaultExecScanQueryOption(opts ...options.ExecuteScanQueryOption) sqlOption {
	return func(c *sqlConnector) {
		c.scanOpts = append(c.scanOpts, opts...)
	}
}

func withClient(client internal.Client) sqlOption {
	return func(c *sqlConnector) {
		c.client = client
	}
}

func Connector(options ...sqlOption) (driver.Connector, error) {
	c := &sqlConnector{
		defaultTxControl: table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		),
	}
	for _, o := range options {
		o(c)
	}
	return c, nil
}

// USE CONNECTOR ONLY
type sqlConnector struct {
	options []ydb.Option
	client  internal.Client

	defaultTxControl *table.TransactionControl

	dataOpts []options.ExecuteDataQueryOption
	scanOpts []options.ExecuteScanQueryOption
}

func (c *sqlConnector) init(ctx context.Context) (err error) {
	var db ydb.Connection
	db, err = ydb.New(
		ctx,
		append(
			c.options,
			ydb.WithTableConfigOption(
				config.WithTrace(
					trace.Table{
						OnPoolClose: func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
							closeCtx := info.Context
							return func(info trace.PoolCloseDoneInfo) {
								go db.Close(closeCtx)
							}
						},
					},
				),
			),
		)...,
	)
	if err != nil {
		return err
	}
	p, ok := db.Table().(interface {
		Pool(context.Context) internal.Client
	})
	if !ok {
		return fmt.Errorf("ydbsql: abnormal type of table client: %T", db.Table())
	}
	c.client = p.Pool(ctx)
	return nil
}

func (c *sqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.client == nil {
		if err := c.init(ctx); err != nil {
			return nil, err
		}
	}
	s, err := c.client.Create(ctx)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, fmt.Errorf("ydbsql: abnormal result of create session")
	}
	return &sqlConn{
		connector: c,
		session:   s,
	}, nil
}

func (c *sqlConnector) Driver() driver.Driver {
	return &sqlDriver{c}
}

// sqlDriver is an adapter to allow the use table client as sql.sqlDriver instance.
// The main purpose of this types is exported is an ability to call Unwrap()
// method on it to receive raw *table.client instance.
type sqlDriver struct {
	c *sqlConnector
}

func (d *sqlDriver) Close(ctx context.Context) error {
	return d.c.client.Close(ctx)
}

// Open returns a new connection to the ydb.
func (d *sqlDriver) Open(string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

func (d *sqlDriver) OpenConnector(string) (driver.Connector, error) {
	return d.c, nil
}
