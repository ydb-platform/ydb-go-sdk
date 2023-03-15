package xsql

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
	"time"

	metaHeaders "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
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

func WithBind(b bind.Bind) ConnectorOption {
	return func(c *Connector) error {
		c.bind = b
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

func WithDisableServerBalancer() ConnectorOption {
	return func(c *Connector) error {
		c.disableServerBalancer = true
		return nil
	}
}

func WithIdleThreshold(idleThreshold time.Duration) ConnectorOption {
	return func(c *Connector) error {
		c.idleThreshold = idleThreshold
		return nil
	}
}

func WithOnClose(f func(connector *Connector)) ConnectorOption {
	return func(c *Connector) error {
		c.onClose = append(c.onClose, f)
		return nil
	}
}

type ydbDriver interface {
	Name() string
	Table() table.Client
	Scripting() scripting.Client
	Scheme() scheme.Client
}

func Open(parent ydbDriver, opts ...ConnectorOption) (_ *Connector, err error) {
	c := &Connector{
		parent:           parent,
		bind:             bind.NoBind(),
		conns:            make(map[*conn]struct{}),
		defaultTxControl: table.DefaultTxControl(),
		defaultQueryMode: DefaultQueryMode,
	}
	for _, opt := range opts {
		if opt != nil {
			if err = opt(c); err != nil {
				return nil, err
			}
		}
	}
	if c.idleThreshold > 0 {
		c.idleStopper = c.idleCloser()
	}
	return c, nil
}

// Connector is a producer of database/sql connections
type Connector struct {
	parent ydbDriver

	bind bind.Bind

	onClose []func(connector *Connector)

	conns    map[*conn]struct{}
	connsMtx sync.RWMutex

	idleStopper func()

	defaultTxControl      *table.TransactionControl
	defaultQueryMode      QueryMode
	defaultDataQueryOpts  []options.ExecuteDataQueryOption
	defaultScanQueryOpts  []options.ExecuteScanQueryOption
	disableServerBalancer bool
	idleThreshold         time.Duration

	trace trace.DatabaseSQL
}

var (
	_ driver.Connector = &Connector{}
	_ io.Closer        = &Connector{}
)

func (c *Connector) idleCloser() (idleStopper func()) {
	var ctx context.Context
	ctx, idleStopper = context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.idleThreshold):
				c.connsMtx.RLock()
				conns := make([]*conn, 0, len(c.conns))
				for cc := range c.conns {
					conns = append(conns, cc)
				}
				c.connsMtx.RUnlock()
				for _, cc := range conns {
					if cc.sinceLastUsage() > c.idleThreshold {
						cc.session.Close(context.Background())
					}
				}
			}
		}
	}()
	return idleStopper
}

func (c *Connector) Close() (err error) {
	defer func() {
		for _, onClose := range c.onClose {
			onClose(c)
		}
	}()
	if c.idleStopper != nil {
		c.idleStopper()
	}
	return nil
}

func (c *Connector) attach(cc *conn) {
	c.connsMtx.Lock()
	defer c.connsMtx.Unlock()
	c.conns[cc] = struct{}{}
}

func (c *Connector) detach(cc *conn) {
	c.connsMtx.Lock()
	defer c.connsMtx.Unlock()
	delete(c.conns, cc)
}

func (c *Connector) Connect(ctx context.Context) (_ driver.Conn, err error) {
	onDone := trace.DatabaseSQLOnConnectorConnect(c.trace, &ctx)
	defer func() {
		onDone(err)
	}()
	if !c.disableServerBalancer {
		ctx = meta.WithAllowFeatures(ctx, metaHeaders.HintSessionBalancer)
	}
	s, err := c.parent.Table().CreateSession(ctx) //nolint
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return newConn(c, s, withDefaultTxControl(c.defaultTxControl),
		withDefaultQueryMode(c.defaultQueryMode),
		withDataOpts(c.defaultDataQueryOpts...),
		withScanOpts(c.defaultScanQueryOpts...),
		withBind(c.bind),
		withTrace(c.trace),
	), nil
}

func (c *Connector) Driver() driver.Driver {
	return &driverWrapper{c: c}
}

type driverWrapper struct {
	c *Connector
}

func (d *driverWrapper) Open(_ string) (driver.Conn, error) {
	return nil, ErrUnsupported
}
