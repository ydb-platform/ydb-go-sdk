package ydbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	ydb "github.com/YandexDatabase/ydb-go-sdk/v2"
	"sync"
	"time"

	"github.com/YandexDatabase/ydb-go-sdk/v2/table"
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

func WithSessionPoolBusyCheckInterval(d time.Duration) ConnectorOption {
	return func(c *connector) {
		c.pool.BusyCheckInterval = d
	}
}

func WithSessionPoolKeepAliveBatchSize(n int) ConnectorOption {
	return func(c *connector) {
		c.pool.KeepAliveBatchSize = n
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
	dialer   ydb.Dialer
	endpoint string

	clientTrace table.ClientTrace

	mu     sync.Mutex
	ready  chan struct{}
	client *table.Client
	pool   table.SessionPool // Used as a template for created connections.

	retryConfig      RetryConfig
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
		if stringer, ok := c.dialer.DriverConfig.Credentials.(fmt.Stringer); ok {
			return nil, fmt.Errorf("dial error: %w (credentials: %s)", err, stringer.String())
		}
		return nil, fmt.Errorf("dial error: %w", err)
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
