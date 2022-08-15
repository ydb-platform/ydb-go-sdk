package xsql

import (
	"context"
	"database/sql/driver"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ConnectorOption func(c *Connector) error

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return func(c *Connector) error {
		c.defaultQueryMode = mode
		return nil
	}
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return func(c *Connector) error {
		c.defaultTxControl = txControl
		return nil
	}
}

func WithDefaultDataQueryOptions(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return func(c *Connector) error {
		c.defaultDataQueryOpts = append(c.defaultDataQueryOpts, opts...)
		return nil
	}
}

func WithDefaultScanQueryOptions(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return func(c *Connector) error {
		c.defaultScanQueryOpts = append(c.defaultScanQueryOpts, opts...)
		return nil
	}
}

func WithTrace(t trace.DatabaseSQL, opts ...trace.DatabaseSQLComposeOption) ConnectorOption {
	return func(c *Connector) error {
		c.trace = c.trace.Compose(t, opts...)
		return nil
	}
}

func Open(d Driver, connection Connection, opts ...ConnectorOption) (_ *Connector, err error) {
	c := &Connector{
		driver:           d,
		connection:       connection,
		defaultTxControl: table.DefaultTxControl(),
		defaultQueryMode: DefaultQueryMode,
	}
	for _, opt := range opts {
		if err = opt(c); err != nil {
			return nil, err
		}
	}
	d.Attach(c)
	return c, nil
}

type Connection interface {
	// Table returns table client
	Table() table.Client

	// Scripting returns scripting client
	Scripting() scripting.Client

	// Close closes connection and clear resources
	Close(ctx context.Context) error
}

type Driver interface {
	driver.Driver

	Attach(c *Connector)
	Detach(c *Connector)
}

// Connector is a producer of database/sql connections
type Connector struct {
	driver     Driver
	connection Connection

	defaultTxControl     *table.TransactionControl
	defaultQueryMode     QueryMode
	defaultDataQueryOpts []options.ExecuteDataQueryOption
	defaultScanQueryOpts []options.ExecuteScanQueryOption

	trace trace.DatabaseSQL
}

var (
	_ driver.Connector = &Connector{}
	_ io.Closer        = &Connector{}
)

func (c *Connector) Close() (err error) {
	defer c.driver.Detach(c)
	return c.connection.Close(context.Background())
}

func (c *Connector) Connection() Connection {
	return c.connection
}

func (c *Connector) Connect(ctx context.Context) (_ driver.Conn, err error) {
	onDone := trace.DatabaseSQLOnConnectorConnect(c.trace, &ctx)
	defer func() {
		onDone(err)
	}()
	s, err := c.connection.Table().CreateSession(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return newConn(c, s,
		withDefaultTxControl(c.defaultTxControl),
		withDefaultQueryMode(c.defaultQueryMode),
		withDataOpts(c.defaultDataQueryOpts...),
		withScanOpts(c.defaultScanQueryOpts...),
		withTrace(c.trace),
	), nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}
