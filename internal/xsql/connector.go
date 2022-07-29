package xsql

import (
	"context"
	"database/sql/driver"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"io"
)

type ConnectorOption func(c *Connector) error

func WithConnection(db ydb.Connection) ConnectorOption {
	return func(c *Connector) error {
		c.db = db
		return nil
	}
}

func WithDataSourceName(dataSourceName string) ConnectorOption {
	return func(c *Connector) error {
		c.dataSourceName = dataSourceName
		return nil
	}
}

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

func Open(parent *ydbDriver, opts ...ConnectorOption) (_ *Connector, err error) {
	c := &Connector{
		parent:           parent,
		defaultTxControl: table.DefaultTxControl(),
		defaultQueryMode: QueryModeDefault,
	}
	for _, opt := range opts {
		if err = opt(c); err != nil {
			return nil, err
		}
	}
	if c.db == nil {
		c.db, err = ydb.Open(context.Background(), c.dataSourceName, c.options...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
	}
	parent.attachConnector(c)
	return c, nil
}

// Connector is a producer of database/sql connections
type Connector struct {
	parent *ydbDriver

	options []ydb.Option

	dataSourceName string
	db             ydb.Connection

	defaultTxControl     *table.TransactionControl
	defaultQueryMode     QueryMode
	defaultDataQueryOpts []options.ExecuteDataQueryOption
	defaultScanQueryOpts []options.ExecuteScanQueryOption
}

var (
	_ driver.Connector = &Connector{}
	_ io.Closer        = &Connector{}
)

func (c *Connector) Close() (err error) {
	defer c.parent.detachConnector(c)
	return c.db.Close(context.Background())
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	s, err := c.db.Table().CreateSession(ctx)
	if err != nil {
		return nil, err
	}
	return newConn(c, s,
		withDefaultTxControl(c.defaultTxControl),
		withDefaultQueryMode(c.defaultQueryMode),
		withDataOpts(c.defaultDataQueryOpts...),
		withScanOpts(c.defaultScanQueryOpts...),
	), nil
}

func (c *Connector) Driver() driver.Driver {
	return c.parent
}
