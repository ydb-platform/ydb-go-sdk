package ydb

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	ratelimiterConfig "github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter/config"
	schemeConfig "github.com/ydb-platform/ydb-go-sdk/v3/scheme/config"
	scriptingConfig "github.com/ydb-platform/ydb-go-sdk/v3/scripting/config"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(ctx context.Context, c *connection) error

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

// WithConnectionString accept connection string like 'grpc[s]://{endpoint}/?database={database}'
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
			return errors.WithStackTrace(errors.NewWithIssues(
				"parse connection string '"+connectionString+"' failed:",
				issues...,
			))
		}
		return nil
	}
}

func RegisterParser(param string, parser func(value string) ([]config.Option, error)) (err error) {
	err = dsn.Register(param, parser)
	if err != nil {
		return errors.WithStackTrace(fmt.Errorf("%w: %s", err, param))
	}
	return nil
}

// WithConnectionTTL defines duration for parking idle connections
// Warning: if defined WithSessionPoolIdleThreshold - idleThreshold must be less than connectionTTL
func WithConnectionTTL(ttl time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithConnectionTTL(ttl))
		return nil
	}
}

func WithEndpoint(endpoint string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithEndpoint(endpoint))
		return nil
	}
}

func WithDatabase(database string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithDatabase(database))
		return nil
	}
}

func WithSecure(secure bool) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithSecure(secure))
		return nil
	}
}

func WithInsecure() Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithSecure(false))
		return nil
	}
}

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

func WithAnonymousCredentials() Option {
	return WithCredentials(
		credentials.NewAnonymousCredentials(credentials.WithSourceInfo("ydb.WithAnonymousCredentials()")),
	)
}

func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (credentials.Credentials, error)) Option {
	return func(ctx context.Context, c *connection) error {
		creds, err := createCredentials(ctx)
		if err != nil {
			return errors.WithStackTrace(err)
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

func WithDialTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithDialTimeout(timeout))
		return nil
	}
}

func With(options ...config.Option) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, options...)
		return nil
	}
}

func MergeOptions(opts ...Option) Option {
	return func(ctx context.Context, c *connection) error {
		for _, o := range opts {
			if err := o(ctx, c); err != nil {
				return errors.WithStackTrace(err)
			}
		}
		return nil
	}
}

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

func WithCertificate(cert *x509.Certificate) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithCertificate(cert))
		return nil
	}
}

func WithCertificatesFromFile(caFile string) Option {
	return func(ctx context.Context, c *connection) error {
		if len(caFile) > 0 && caFile[0] == '~' {
			home, err := os.UserHomeDir()
			if err != nil {
				return errors.WithStackTrace(err)
			}
			caFile = filepath.Join(home, caFile[1:])
		}
		bytes, err := ioutil.ReadFile(filepath.Clean(caFile))
		if err != nil {
			return errors.WithStackTrace(err)
		}
		if err = WithCertificatesFromPem(bytes)(ctx, c); err != nil {
			return errors.WithStackTrace(err)
		}
		return nil
	}
}

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
			return errors.WithStackTrace(err)
		}
		return nil
	}
}

func WithTableConfigOption(option tableConfig.Option) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, option)
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithSizeLimit(sizeLimit))
		return nil
	}
}

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

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithKeepAliveTimeout(keepAliveTimeout))
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithCreateSessionTimeout(createSessionTimeout))
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithDeleteTimeout(deleteTimeout))
		return nil
	}
}

// WithRecoverPanic specified flag for use panic recover on calls user-defined funcs
func WithRecoverPanic() Option {
	return func(ctx context.Context, c *connection) error {
		c.recoverPanic = true
		return nil
	}
}

// WithRecoverPanicWriter specified `io.Writer` for logging panic details
func WithRecoverPanicWriter(w io.Writer) Option {
	return func(ctx context.Context, c *connection) error {
		c.recoverPanicWriter = w
		return nil
	}
}

// WithExitCodeOnPanic specified code for exit on panic
// If nil - no exiting on panic
func WithExitCodeOnPanic(code int) Option {
	return func(ctx context.Context, c *connection) error {
		c.exitCodeOnPanic = &code
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
						trace.WithTableRecoverPanic(c.recoverPanic),
						trace.WithTableRecoverPanicWriter(c.recoverPanicWriter),
						trace.WithTableExitCodeOnPanic(c.exitCodeOnPanic),
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
						trace.WithScriptingRecoverPanic(c.recoverPanic),
						trace.WithScriptingRecoverPanicWriter(c.recoverPanicWriter),
						trace.WithScriptingExitCodeOnPanic(c.exitCodeOnPanic),
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
						trace.WithSchemeRecoverPanic(c.recoverPanic),
						trace.WithSchemeRecoverPanicWriter(c.recoverPanicWriter),
						trace.WithSchemeExitCodeOnPanic(c.exitCodeOnPanic),
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
						trace.WithCoordinationRecoverPanic(c.recoverPanic),
						trace.WithCoordinationRecoverPanicWriter(c.recoverPanicWriter),
						trace.WithCoordinationExitCodeOnPanic(c.exitCodeOnPanic),
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
						trace.WithRatelimiterRecoverPanic(c.recoverPanic),
						trace.WithRatelimiterRecoverPanicWriter(c.recoverPanicWriter),
						trace.WithRatelimiterExitCodeOnPanic(c.exitCodeOnPanic),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}

func WithRatelimiterOptions(opts ...ratelimiterConfig.Option) Option {
	return func(ctx context.Context, c *connection) error {
		c.ratelimiterOptions = append(c.ratelimiterOptions, opts...)
		return nil
	}
}

// WithTraceDiscovery returns discovery trace option
func WithTraceDiscovery(t trace.Discovery, opts ...trace.DiscoveryComposeOption) Option {
	return func(ctx context.Context, c *connection) error {
		c.discoveryOptions = append(
			c.discoveryOptions,
			discoveryConfig.WithTrace(
				t,
				append(
					[]trace.DiscoveryComposeOption{
						trace.WithDiscoveryRecoverPanic(c.recoverPanic),
						trace.WithDiscoveryRecoverPanicWriter(c.recoverPanicWriter),
						trace.WithDiscoveryExitCodeOnPanic(c.exitCodeOnPanic),
					},
					opts...,
				)...,
			),
		)
		return nil
	}
}
