package ydbsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ConnectorOption func(*connector) error

func WithDialer(d dial.Dialer) ConnectorOption {
	return func(c *connector) error {
		c.dialer = d
		if c.dialer.Config == nil {
			c.dialer.Config = config.New()
		}
		return nil
	}
}

func withClient(pool internal.ClientAsPool) ConnectorOption {
	return func(c *connector) error {
		c.tbl = pool
		return nil
	}
}

func WithConnectParams(params ydb.ConnectParams) ConnectorOption {
	return func(c *connector) error {
		c.endpoint = params.Endpoint()
		if c.dialer.Config == nil {
			c.dialer.Config = &config.Config{}
		}
		c.dialer.Config.Database = params.Database()
		if params.UseTLS() {
			if c.dialer.TLSConfig == nil {
				c.dialer.TLSConfig = &tls.Config{}
			}
		} else {
			c.dialer.TLSConfig = nil
		}
		return nil
	}
}

func WithTLSConfig(tlsConfig *tls.Config) ConnectorOption {
	return func(c *connector) error {
		c.dialer.TLSConfig = tlsConfig
		return nil
	}
}

func WithCertificates(certPool *x509.CertPool) ConnectorOption {
	return func(c *connector) error {
		if c.dialer.TLSConfig == nil {
			c.dialer.TLSConfig = &tls.Config{}
		}
		c.dialer.TLSConfig.RootCAs = certPool
		return nil
	}
}

func WithCertificatesFromFile(caFile string) ConnectorOption {
	return func(c *connector) error {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return err
		}
		err = credentials.AppendCertsFromFile(certPool, caFile)
		if err != nil {
			return err
		}
		if c.dialer.TLSConfig == nil {
			c.dialer.TLSConfig = &tls.Config{}
		}
		c.dialer.TLSConfig.RootCAs = certPool
		return nil
	}
}

func WithDialTimeout(timeout time.Duration) ConnectorOption {
	return func(c *connector) error {
		c.dialer.Timeout = timeout
		return nil
	}
}

func WithCertificatesFromPem(pem []byte) ConnectorOption {
	return func(c *connector) error {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return err
		}
		if ok := certPool.AppendCertsFromPEM(pem); !ok {
			return err
		}
		if c.dialer.TLSConfig == nil {
			c.dialer.TLSConfig = &tls.Config{}
		}
		c.dialer.TLSConfig.RootCAs = certPool
		return nil
	}
}

func WithConnectionString(connection string) ConnectorOption {
	return WithConnectParams(ydb.MustConnectionString(connection))
}

func WithEndpoint(addr string) ConnectorOption {
	return func(c *connector) error {
		c.endpoint = addr
		return nil
	}
}

func WithDriverConfig(config config.Config) ConnectorOption {
	return func(c *connector) error {
		*(c.dialer.Config) = config
		return nil
	}
}

func WithCredentials(creds credentials.Credentials) ConnectorOption {
	return func(c *connector) error {
		c.dialer.Config.Credentials = creds
		return nil
	}
}

func WithDatabase(db string) ConnectorOption {
	return func(c *connector) error {
		c.dialer.Config.Database = db
		return nil
	}
}

func WithTraceDriver(t trace.Driver) ConnectorOption {
	return func(c *connector) error {
		c.dialer.Config.Trace = t
		return nil
	}
}

func WithTraceTable(t trace.Table) ConnectorOption {
	return func(c *connector) error {
		c.trace = t
		return nil
	}
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return func(c *connector) error {
		c.defaultTxControl = txControl
		return nil
	}
}

func WithDefaultExecDataQueryOption(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return func(c *connector) error {
		c.dataOpts = append(c.dataOpts, opts...)
		return nil
	}
}

func WithDefaultExecScanQueryOption(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return func(c *connector) error {
		c.scanOpts = append(c.scanOpts, opts...)
		return nil
	}
}

func Connector(opts ...ConnectorOption) (driver.Connector, error) {
	c := &connector{
		dialer: dial.Dialer{
			Config: config.New(),
		},
		defaultTxControl: table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		),
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if c.dialer.TLSConfig != nil {
		if caFile, hasUserCA := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); hasUserCA || c.dialer.TLSConfig.RootCAs == nil {
			certs, err := x509.SystemCertPool()
			if err != nil {
				return nil, err
			}
			if hasUserCA {
				if err := credentials.AppendCertsFromFile(certs, caFile); err != nil {
					return nil, fmt.Errorf("cannot load certificates from file '%s' by Env['YDB_SSL_ROOT_CERTIFICATES_FILE']: %v", caFile, err)
				}
			}
			c.dialer.TLSConfig.RootCAs = certs
		}
	}
	return c, nil
}

// USE CONNECTOR ONLY
type connector struct {
	dialer   dial.Dialer
	endpoint string

	trace trace.Table

	mu  sync.Mutex
	tbl internal.ClientAsPool // Used as a template for created connections.

	defaultTxControl *table.TransactionControl

	dataOpts []options.ExecuteDataQueryOption
	scanOpts []options.ExecuteScanQueryOption
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
	if assert.IsNil(c.tbl) {
		c.tbl, err = c.dial(ctx)
	}
	//c.tbl.Builder = c.client
	return
}

func (c *connector) dial(ctx context.Context) (internal.ClientAsPool, error) {
	d, err := c.dialer.Dial(ctx, c.endpoint)
	if err != nil {
		if c == nil {
			return nil, fmt.Errorf("nil connector")
		}
		if c.dialer.Config == nil {
			return nil, fmt.Errorf("nil driver config")
		}
		if c.dialer.Config.Credentials == nil {
			return nil, fmt.Errorf("nil credentials")
		}
		if stringer, ok := c.dialer.Config.Credentials.(fmt.Stringer); ok {
			return nil, fmt.Errorf("dial error: %w (credentials: %s)", err, stringer.String())
		}
		return nil, fmt.Errorf("dial error: %w", err)
	}
	return internal.NewClientAsPool(d, ContextTableConfig(ctx)), nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	s, err := c.tbl.Create(ctx)
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

func (c *connector) unwrap(ctx context.Context) (table.Client, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}
	return c.tbl, nil
}

// Driver is an adapter to allow the use table client as sql.Driver instance.
// The main purpose of this types is exported is an ability to call Unwrap()
// method on it to receive raw *table.client instance.
type Driver struct {
	c *connector
}

func (d *Driver) Close(ctx context.Context) error {
	return d.c.tbl.Close(ctx)
}

// Open returns a new connection to the ydb.
func (d *Driver) Open(string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

func (d *Driver) OpenConnector(string) (driver.Connector, error) {
	return d.c, nil
}

func (d *Driver) Unwrap(ctx context.Context) (table.Client, error) {
	return d.c.unwrap(ctx)
}
