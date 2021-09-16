package ydbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"
	driver2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var (
	DefaultIdleThreshold        = 5 * time.Second
	DefaultSessionPoolSizeLimit = 1 << 12
)

type ConnectorOption func(*connector)

func WithDialer(d dial.Dialer) ConnectorOption {
	return func(c *connector) {
		c.dialer = d
		if c.dialer.DriverConfig == nil {
			c.dialer.DriverConfig = new(driver2.Config)
		}
	}
}

func WithClient(client *table.Client) ConnectorOption {
	return func(c *connector) {
		c.client = client
	}
}

func WithEndpoint(addr string) ConnectorOption {
	return func(c *connector) {
		c.endpoint = addr
	}
}

func WithDriverConfig(config driver2.Config) ConnectorOption {
	return func(c *connector) {
		*(c.dialer.DriverConfig) = config
	}
}

func WithCredentials(creds credentials.Credentials) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Credentials = creds
	}
}

func WithDatabase(db string) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Database = db
	}
}

func WithDriverTrace(t trace.DriverTrace) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Trace = t
	}
}

func WithClientTrace(t table.ClientTrace) ConnectorOption {
	return func(c *connector) {
		c.clientTrace = t
	}
}

func WithSessionPoolTrace(t table.SessionPoolTrace) ConnectorOption {
	return func(c *connector) {
		c.pool.Trace = t
	}
}

func WithSessionPoolSizeLimit(n int) ConnectorOption {
	return func(c *connector) {
		c.pool.SizeLimit = n
	}
}

func WithSessionPoolIdleThreshold(d time.Duration) ConnectorOption {
	return func(c *connector) {
		c.pool.IdleThreshold = d
	}
}

func WithSessionPoolKeepAliveTimeout(d time.Duration) ConnectorOption {
	return func(c *connector) {
		c.pool.KeepAliveTimeout = d
	}
}

func WithSessionPoolCreateSessionTimeout(d time.Duration) ConnectorOption {
	return func(c *connector) {
		c.pool.CreateSessionTimeout = d
	}
}

func WithSessionPoolDeleteTimeout(d time.Duration) ConnectorOption {
	return func(c *connector) {
		c.pool.DeleteTimeout = d
	}
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return func(c *connector) {
		c.defaultTxControl = txControl
	}
}

func WithDefaultExecDataQueryOption(opts ...table.ExecuteDataQueryOption) ConnectorOption {
	return func(c *connector) {
		c.dataOpts = append(c.dataOpts, opts...)
	}
}

func WithDefaultExecScanQueryOption(opts ...table.ExecuteScanQueryOption) ConnectorOption {
	return func(c *connector) {
		c.scanOpts = append(c.scanOpts, opts...)
	}
}

func Connector(opts ...ConnectorOption) driver.Connector {
	c := &connector{
		dialer: dial.Dialer{
			DriverConfig: new(driver2.Config),
		},
		defaultTxControl: table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// USE CONNECTOR ONLY
type connector struct {
	dialer   dial.Dialer
	endpoint string

	clientTrace table.ClientTrace

	mu     sync.Mutex
	ready  chan struct{}
	client *table.Client
	pool   table.SessionPool // Used as a template for created connections.

	defaultTxControl *table.TransactionControl

	dataOpts []table.ExecuteDataQueryOption
	scanOpts []table.ExecuteScanQueryOption
}

func (c *connector) init(ctx context.Context) (err error) {
	// in driver database/sql/sql.go:1228 connect run under mutex, but don't rely on it here
	c.mu.Lock()
	defer c.mu.Unlock()

	// Setup some more on less generic reasonable pool limit to prevent
	// session overflow on the YDB servers.
	//
	// Note that it must be controlled from outside by making
	// database/sql.DB.SetMaxIdleConns() call. Unfortunately, we can not
	// receive that limit here and we do not want to force user to
	// configure it twice (and pass it as an option to connector).
	if c.pool.SizeLimit == 0 {
		c.pool.SizeLimit = DefaultSessionPoolSizeLimit
	}
	if c.pool.IdleThreshold == 0 {
		c.pool.IdleThreshold = DefaultIdleThreshold
	}
	if c.client == nil {
		c.client, err = c.dial(ctx)
	}
	c.pool.Builder = c.client
	return
}

func (c *connector) dial(ctx context.Context) (*table.Client, error) {
	d, err := c.dialer.Dial(ctx, c.endpoint)
	if err != nil {
		if c == nil {
			panic("nil connector")
		}
		if c.dialer.DriverConfig == nil {
			panic("nil driver config")
		}
		if c.dialer.DriverConfig.Credentials == nil {
			panic("nil credentials")
		}
		if stringer, ok := c.dialer.DriverConfig.Credentials.(fmt.Stringer); ok {
			return nil, fmt.Errorf("dial error: %w (credentials: %s)", err, stringer.String())
		}
		return nil, fmt.Errorf("dial error: %w", err)
	}
	return table.NewClient(d, table.WithClientTraceOption(c.clientTrace)), nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	s, err := c.pool.Create(ctx)
	if err != nil {
		return nil, err
	}
	if s == nil {
		panic("ydbsql: abnormal result of pool.Create()")
	}
	return &conn{
		connector: c,
		session:   s,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{c}
}

func (c *connector) unwrap(ctx context.Context) (*table.Client, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	return c.client, nil
}

// Driver is an adapter to allow the use table client as sql.Driver instance.
// The main purpose of this types is exported is an ability to call Unwrap()
// method on it to receive raw *table.Client instance.
type Driver struct {
	c *connector
}

func (d *Driver) Close() error {
	_ = d.c.pool.Close(context.Background())
	return d.c.client.Close()
}

// Open returns a new connection to the ydb.
func (d *Driver) Open(string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

func (d *Driver) OpenConnector(string) (driver.Connector, error) {
	return d.c, nil
}

func (d *Driver) Unwrap(ctx context.Context) (*table.Client, error) {
	return d.c.unwrap(ctx)
}
