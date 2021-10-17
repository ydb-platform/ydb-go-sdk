package ydbsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(*connector)

func With(options ...ydb.Option) Option {
	return func(c *connector) {
		c.options = append(c.options, options...)
	}
}

func WithDefaultTxControl(txControl *table.TransactionControl) Option {
	return func(c *connector) {
		c.defaultTxControl = txControl
	}
}

func WithDefaultExecDataQueryOption(opts ...options.ExecuteDataQueryOption) Option {
	return func(c *connector) {
		c.dataOpts = append(c.dataOpts, opts...)
	}
}

func WithDefaultExecScanQueryOption(opts ...options.ExecuteScanQueryOption) Option {
	return func(c *connector) {
		c.scanOpts = append(c.scanOpts, opts...)
	}
}

func withClient(client internal.Client) Option {
	return func(c *connector) {
		c.client = client
	}
}

func Connector(options ...Option) (driver.Connector, error) {
	c := &connector{
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
type connector struct {
	options []ydb.Option
	client  internal.Client

	defaultTxControl *table.TransactionControl

	dataOpts []options.ExecuteDataQueryOption
	scanOpts []options.ExecuteScanQueryOption
}

func (c *connector) init(ctx context.Context) (err error) {
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

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
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

func (c *connector) Driver() driver.Driver {
	return &Driver{c}
}

// Driver is an adapter to allow the use table client as sql.Driver instance.
// The main purpose of this types is exported is an ability to call Unwrap()
// method on it to receive raw *table.client instance.
type Driver struct {
	c *connector
}

func (d *Driver) Close(ctx context.Context) error {
	return d.c.client.Close(ctx)
}

// Open returns a new connection to the ydb.
func (d *Driver) Open(string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

func (d *Driver) OpenConnector(string) (driver.Connector, error) {
	return d.c, nil
}
