package ydbsql

import (
	"context"
	"database/sql/driver"
	"sync"
	"time"

	ydb "github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

var (
	DefaultIdleThreshold        = 5 * time.Second
	DefaultSessionPoolSizeLimit = 1 << 12
)

type ConnectorOption func(*connector)

func WithDialer(d ydb.Dialer) ConnectorOption {
	return func(c *connector) {
		c.dialer = d
	}
}

// NOTE: must be called after all other options.
func WithClient(client *table.Client) ConnectorOption {
	return func(c *connector) {
		c.prepare(func(_ context.Context) (*table.Client, error) {
			return client, nil
		})
	}
}

func WithEndpoint(addr string) ConnectorOption {
	return func(c *connector) {
		c.endpoint = addr
	}
}

func WithDriverConfig(config ydb.DriverConfig) ConnectorOption {
	return func(c *connector) {
		*(c.dialer.DriverConfig) = config
	}
}

func WithCredentials(creds ydb.Credentials) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Credentials = creds
	}
}

func WithDatabase(db string) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Database = db
	}
}

func WithDriverTrace(t ydb.DriverTrace) ConnectorOption {
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

func WithSessionPoolIdleThreshold(d time.Duration) ConnectorOption {
	return func(c *connector) {
		c.pool.IdleThreshold = d
	}
}

func WithSessionPoolBusyCheckInterval(d time.Duration) ConnectorOption {
	return func(c *connector) {
		c.pool.BusyCheckInterval = d
	}
}

func WithSessionPoolSizeLimit(n int) ConnectorOption {
	return func(c *connector) {
		c.pool.SizeLimit = n
	}
}

func WithMaxRetries(n int) ConnectorOption {
	return func(c *connector) {
		if n >= 0 {
			c.retryConfig.MaxRetries = n
		}
	}
}

func WithRetryBackoff(b ydb.Backoff) ConnectorOption {
	return func(c *connector) {
		c.retryConfig.Backoff = b
	}
}

var retryChecker = ydb.RetryChecker{
	// NOTE: we do not want to retry not found prepared statement
	// errors.
	//
	// In other case we would just burn the CPU looping up to max
	// retry attempts times – we are not making any Prepare() calls
	// in the retry callbacks.
	RetryNotFound: false,
}

func Connector(opts ...ConnectorOption) driver.Connector {
	c := &connector{
		dialer: ydb.Dialer{
			DriverConfig: new(ydb.DriverConfig),
		},
		retryConfig: RetryConfig{
			MaxRetries:   ydb.DefaultMaxRetries,
			Backoff:      ydb.DefaultBackoff,
			RetryChecker: retryChecker,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// USE CONNECTOR ONLY
type connector struct {
	dialer   ydb.Dialer
	endpoint string

	clientTrace table.ClientTrace

	once   sync.Once
	ready  chan struct{}
	err    error // sticky error.
	client *table.Client
	pool   table.SessionPool // Used as a template for created connections.

	retryConfig RetryConfig
}

func (c *connector) init(ctx context.Context) error {
	c.prepare(c.dial)
	select {
	case <-c.ready:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) prepare(dial func(context.Context) (*table.Client, error)) {
	c.once.Do(func() {
		c.ready = make(chan struct{})
		go func() {
			defer close(c.ready)
			ctx := context.TODO()
			c.client, c.err = dial(ctx)
			if c.err != nil {
				return
			}
			c.pool.Builder = c.client

			// Setup some more on less generic reasonable pool limit to prevent
			// session overflow on the YDB servers.
			//
			// Note that it must be controlled from outside by making
			// database/sql.DB.SetMaxIdleConns() call. Unfortunatly, we can not
			// receive that limit here and we do not want to force user to
			// configure it twice (and pass it as an option to connector).
			c.pool.SizeLimit = DefaultSessionPoolSizeLimit

			if c.pool.IdleThreshold == 0 {
				c.pool.IdleThreshold = DefaultIdleThreshold
			}
		}()
	})
}

func (c *connector) dial(ctx context.Context) (*table.Client, error) {
	d, err := c.dialer.Dial(ctx, c.endpoint)
	if err != nil {
		return nil, err
	}
	return &table.Client{
		Driver: d,
		Trace:  c.clientTrace,
	}, nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	s, err := c.pool.Create(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{
		session:     s,
		pool:        &c.pool,
		retryConfig: &c.retryConfig,
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
// The main purpose of this type is exported is an ability to call Unwrap()
// method on it to receive raw *table.Client instance.
type Driver struct {
	c *connector
}

func (d *Driver) Close() error {
	_ = d.c.pool.Close(context.Background())
	return d.c.client.Driver.Close()
}

// Open returns a new connection to the ydb.
func (d *Driver) Open(name string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return d.c, nil
}

func (d *Driver) Unwrap(ctx context.Context) (*table.Client, error) {
	return d.c.unwrap(ctx)
}
