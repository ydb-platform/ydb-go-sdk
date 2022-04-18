package ydb

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	coordinationConfig "github.com/ydb-platform/ydb-go-sdk/v3/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	ratelimiterConfig "github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter/config"
	schemeConfig "github.com/ydb-platform/ydb-go-sdk/v3/scheme/config"
	scriptingConfig "github.com/ydb-platform/ydb-go-sdk/v3/scripting/config"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Option contains configuration values for Connection
type Option func(ctx context.Context, c *connection) error

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		credentials.NewAccessTokenCredentials(
			accessToken,
			credentials.WithSourceInfo(
				"ydb.WithAccessTokenCredentials(accessToken)", // hide access token for logs
			),
		),
	)
}

// WithUserAgent add provided user agent value to all api requests
func WithUserAgent(userAgent string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithUserAgent(userAgent))
		return nil
	}
}

func WithRequestsType(requestsType string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithRequestsType(requestsType))
		return nil
	}
}

// WithConnectionString accept connection string like
//  grpc[s]://{endpoint}/?database={database}
//
// Warning: WithConnectionString will be removed at next major release
// (connection string will be required string param of ydb.Open)
func WithConnectionString(connectionString string) Option {
	return func(ctx context.Context, c *connection) error {
		var (
			urls = []string{
				connectionString,
				"grpcs://" + connectionString,
			}
			issues = make([]error, 0, len(urls))
		)
		for _, url := range urls {
			options, err := dsn.Parse(url)
			if err == nil {
				c.options = append(c.options, options...)
				return nil
			}
			issues = append(issues, err)
		}
		if len(issues) > 0 {
			return xerrors.WithStackTrace(xerrors.NewWithIssues(
				"parse connection string '"+connectionString+"' failed:",
				issues...,
			))
		}
		return nil
	}
}

// WithConnectionTTL defines duration for parking idle connections
//
// Warning: if defined WithSessionPoolIdleThreshold - idleThreshold must be less than connectionTTL
func WithConnectionTTL(ttl time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithConnectionTTL(ttl))
		return nil
	}
}

// WithEndpoint defines endpoint option
//
// Deprecated: use WithConnectionString or dsn package instead
func WithEndpoint(endpoint string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithEndpoint(endpoint))
		return nil
	}
}

// WithDatabase defines database option
//
// Deprecated: use WithConnectionString or dsn package instead
func WithDatabase(database string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithDatabase(database))
		return nil
	}
}

// WithSecure defines secure option
//
// Deprecated: use WithConnectionString or dsn package instead
func WithSecure(secure bool) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithSecure(secure))
		return nil
	}
}

// WithInsecure defines secure option
//
// Deprecated: use WithConnectionString or dsn package instead
func WithInsecure() Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithSecure(false))
		return nil
	}
}

// WithMinTLSVersion set minimum TLS version acceptable for connections
func WithMinTLSVersion(minVersion uint16) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithMinTLSVersion(minVersion))
		return nil
	}
}

func WithTLSSInsecureSkipVerify() Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithTLSSInsecureSkipVerify())
		return nil
	}
}

// WithLogger add enables logging for selected tracing events.
// See trace package documentation for details.
func WithLogger(details trace.Details, opts ...LoggerOption) Option {
	loggerOpts := make([]logger.Option, 0, len(opts))
	for _, o := range opts {
		loggerOpts = append(loggerOpts, logger.Option(o))
	}

	l := logger.New(loggerOpts...)
	return MergeOptions(
		WithTraceDriver(log.Driver(l, details)),
		WithTraceTable(log.Table(l, details)),
		WithTraceScripting(log.Scripting(l, details)),
		WithTraceScheme(log.Scheme(l, details)),
		WithTraceCoordination(log.Coordination(l, details)),
		WithTraceRatelimiter(log.Ratelimiter(l, details)),
		WithTraceDiscovery(log.Discovery(l, details)),
	)
}

// WithAnonymousCredentials force to make requests withou authentication.
func WithAnonymousCredentials() Option {
	return WithCredentials(
		credentials.NewAnonymousCredentials(credentials.WithSourceInfo("ydb.WithAnonymousCredentials()")),
	)
}

// WithCreateCredentialsFunc add callback funcion to provide requests credentials
func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (credentials.Credentials, error)) Option {
	return func(ctx context.Context, c *connection) error {
		creds, err := createCredentials(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		c.options = append(c.options, config.WithCredentials(creds))
		return nil
	}
}

// WithCredentials in conjunction with Connection.With function prohibit reuse of conn pool.
// Thus, Connection.With will effectively create totally separate Connection.
func WithCredentials(c credentials.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return c, nil
	})
}

func WithBalancer(balancer balancer.Balancer) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithBalancer(balancer))
		return nil
	}
}

// WithDialTimeout sets timeout for establishing new connection to cluster
func WithDialTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithDialTimeout(timeout))
		return nil
	}
}

// With collects additional configuration options.
// This option does not replace collected option, instead it will appen provided options.
func With(options ...config.Option) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, options...)
		return nil
	}
}

// MergeOptions concatentaes provided options to one cumulative value.
func MergeOptions(opts ...Option) Option {
	return func(ctx context.Context, c *connection) error {
		for _, o := range opts {
			if err := o(ctx, c); err != nil {
				return xerrors.WithStackTrace(err)
			}
		}
		return nil
	}
}

// WithDiscoveryInterval sets interval between cluster discovery calls.
func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.discoveryOptions = append(c.discoveryOptions, discoveryConfig.WithInterval(discoveryInterval))
		return nil
	}
}

// WithTraceDriver returns deadline which has associated Driver with it.
func WithTraceDriver(trace trace.Driver, opts ...trace.DriverComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithTrace(trace, opts...))
		return nil
	}
}

// WithCertificate provides custom CA certificate.
func WithCertificate(cert *x509.Certificate) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithCertificate(cert))
		return nil
	}
}

// WithCertificate provides filepath to load custom CA certificates.
func WithCertificatesFromFile(caFile string) Option {
	return func(ctx context.Context, c *connection) error {
		if len(caFile) > 0 && caFile[0] == '~' {
			home, err := os.UserHomeDir()
			if err != nil {
				return xerrors.WithStackTrace(err)
			}
			caFile = filepath.Join(home, caFile[1:])
		}
		bytes, err := ioutil.ReadFile(filepath.Clean(caFile))
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		if err = WithCertificatesFromPem(bytes)(ctx, c); err != nil {
			return xerrors.WithStackTrace(err)
		}
		return nil
	}
}

// WithCertificate provides PEM encoded custom CA certificates.
func WithCertificatesFromPem(bytes []byte) Option {
	return func(ctx context.Context, c *connection) error {
		if ok, err := func(bytes []byte) (ok bool, err error) {
			var cert *x509.Certificate
			for len(bytes) > 0 {
				var block *pem.Block
				block, bytes = pem.Decode(bytes)
				if block == nil {
					break
				}
				if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
					continue
				}
				certBytes := block.Bytes
				cert, err = x509.ParseCertificate(certBytes)
				if err != nil {
					continue
				}
				_ = WithCertificate(cert)(ctx, c)
				ok = true
			}
			return
		}(bytes); !ok {
			return xerrors.WithStackTrace(err)
		}
		return nil
	}
}

// WithTableConfigOption collects additional configuration options for table.Client.
// This option does not replace collected option, instead it will appen provided options.
func WithTableConfigOption(option tableConfig.Option) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, option)
		return nil
	}
}

// WithSessionPoolSizeLimit set max size of internal sessions pool in table.Client
func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithSizeLimit(sizeLimit))
		return nil
	}
}

// WithSessionPoolKeepAliveMinSize set minimum sessions should be keeped alive in table.Client
func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithKeepAliveMinSize(keepAliveMinSize))
		return nil
	}
}

// WithSessionPoolIdleThreshold defines keep-alive interval for idle sessions
// Warning: if defined WithConnectionTTL - idleThreshold must be less than connectionTTL
func WithSessionPoolIdleThreshold(idleThreshold time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithIdleThreshold(idleThreshold))
		return nil
	}
}

// WithSessionPoolKeepAliveTimeout set timeout of keep alive requests for session in table.Client
func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithKeepAliveTimeout(keepAliveTimeout))
		return nil
	}
}

// WithSessionPoolCreateSessionTimeout set timeout for new session creation process in table.Client
func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithCreateSessionTimeout(createSessionTimeout))
		return nil
	}
}

// WithSessionPoolDeleteTimeout set timeout to gracefully close deleting session in table.Client
func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithDeleteTimeout(deleteTimeout))
		return nil
	}
}

// WithPanicCallback specified behavior on panic
// Warning: WithPanicCallback must be defined on start of all options
// (before `WithTrace{Driver,Table,Scheme,Scripting,Coordination,Ratelimiter}` and other options)
// If not defined - panic would not intercept with driver
func WithPanicCallback(panicCallback func(e interface{})) Option {
	return func(ctx context.Context, c *connection) error {
		c.panicCallback = panicCallback
		c.options = append(c.options, config.WithPanicCallback(panicCallback))
		return nil
	}
}

// WithTraceTable returns table trace option
func WithTraceTable(t trace.Table, opts ...trace.TableComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(
			c.tableOptions,
			tableConfig.WithTrace(
				t,
				append(
					[]trace.TableComposeOption{
						trace.WithTablePanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}

// WithTraceScripting scripting trace option
func WithTraceScripting(t trace.Scripting, opts ...trace.ScriptingComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.scriptingOptions = append(
			c.scriptingOptions,
			scriptingConfig.WithTrace(
				t,
				append(
					[]trace.ScriptingComposeOption{
						trace.WithScriptingPanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}

// WithTraceScheme returns scheme trace option
func WithTraceScheme(t trace.Scheme, opts ...trace.SchemeComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.schemeOptions = append(
			c.schemeOptions,
			schemeConfig.WithTrace(
				t,
				append(
					[]trace.SchemeComposeOption{
						trace.WithSchemePanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}

// WithTraceCoordination returns coordination trace option
func WithTraceCoordination(t trace.Coordination, opts ...trace.CoordinationComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.coordinationOptions = append(
			c.coordinationOptions,
			coordinationConfig.WithTrace(
				t,
				append(
					[]trace.CoordinationComposeOption{
						trace.WithCoordinationPanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}

// WithTraceRatelimiter returns ratelimiter trace option
func WithTraceRatelimiter(t trace.Ratelimiter, opts ...trace.RatelimiterComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.ratelimiterOptions = append(
			c.ratelimiterOptions,
			ratelimiterConfig.WithTrace(
				t,
				append(
					[]trace.RatelimiterComposeOption{
						trace.WithRatelimiterPanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}

// WithRatelimiterOptions returns reatelimiter option
func WithRatelimiterOptions(opts ...ratelimiterConfig.Option) Option {
	return func(ctx context.Context, c *connection) error {
		c.ratelimiterOptions = append(c.ratelimiterOptions, opts...)
		return nil
	}
}

// WithTraceDiscovery adds configured discovery tracer to Connection
func WithTraceDiscovery(t trace.Discovery, opts ...trace.DiscoveryComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.discoveryOptions = append(
			c.discoveryOptions,
			discoveryConfig.WithTrace(
				t,
				append(
					[]trace.DiscoveryComposeOption{
						trace.WithDiscoveryPanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}

// Private technical options for correct copies processing

func withOnClose(onClose func(c *connection)) Option {
	return func(ctx context.Context, c *connection) error {
		c.onClose = append(c.onClose, onClose)
		return nil
	}
}

func withConnPool(pool conn.Pool) Option {
	return func(ctx context.Context, c *connection) error {
		c.pool = pool
		return pool.Take(ctx)
	}
}
