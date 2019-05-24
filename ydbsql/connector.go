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
	DefaultIdleThreshold = 5 * time.Second
)

type ConnectorOption func(*connector)

func WithClient(client *table.Client) ConnectorOption {
	return func(c *connector) {
		c.prepare(func(_ context.Context) (*table.Client, error) {
			return client, nil
		})
	}
}

func WithCredentials(creds ydb.Credentials) ConnectorOption {
	return func(c *connector) {
		c.dialer.DriverConfig.Credentials = creds
	}
}

func WithEndpoint(addr string) ConnectorOption {
	return func(c *connector) {
		c.endpoint = addr
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

func WithMaxRetries(n int) ConnectorOption {
	return func(c *connector) {
		if n >= 0 {
			c.retry.MaxRetries = n
		}
	}
}

func WithRetryChecker(r ydb.RetryChecker) ConnectorOption {
	return func(c *connector) {
		c.retry.RetryChecker = r
	}
}

func WithRetryBackoff(b ydb.Backoff) ConnectorOption {
	return func(c *connector) {
		c.retry.Backoff = b
	}
}

func Connector(opts ...ConnectorOption) driver.Connector {
	c := &connector{
		dialer: ydb.Dialer{
			DriverConfig: new(ydb.DriverConfig),
		},
		retry: retryer{
			MaxRetries: ydb.DefaultMaxRetries,
			Backoff:    ydb.DefaultBackoff,
			RetryChecker: ydb.RetryChecker{
				// NOTE: we do not want to retry not found prepared statement
				// errors.
				//
				// In other case we would just burn the CPU looping up to max
				// retry attempts times – we do not making and Prepare() calls
				// in the retry callbacks.
				RetryNotFound: false,
			},
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

	retry retryer
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
			c.pool.SizeLimit = -1
			c.pool.KeepAliveBatchSize = -1
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
	s, err := c.client.CreateSession(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{
		session: s,
		pool:    &c.pool,
		retry:   &c.retry,
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
	d.c.pool.Close(context.Background())
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
