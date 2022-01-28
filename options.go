package ydb

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

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

func WithUserAgent(userAgent string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithUserAgent(userAgent))
		return nil
	}
}

func WithConnectionString(dsn string) Option {
	return func(ctx context.Context, c *connection) error {
		params, err := ConnectionString(dsn)
		if err != nil {
			return err
		}
		return WithConnectParams(params)(ctx, c)
	}
}

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

type Level logger.Level

const (
	QUIET = Level(logger.QUIET)
	TRACE = Level(logger.TRACE)
	DEBUG = Level(logger.DEBUG)
	INFO  = Level(logger.INFO)
	WARN  = Level(logger.WARN)
	ERROR = Level(logger.ERROR)
	FATAL = Level(logger.FATAL)
)

type LoggerOption logger.Option

func WithNamespace(namespace string) LoggerOption {
	return LoggerOption(logger.WithNamespace(namespace))
}

func WithMinLevel(minLevel Level) LoggerOption {
	return LoggerOption(logger.WithMinLevel(logger.Level(minLevel)))
}

func WithNoColor(b bool) LoggerOption {
	return LoggerOption(logger.WithNoColor(b))
}

func WithExternalLogger(external log.Logger) LoggerOption {
	return LoggerOption(logger.WithExternalLogger(external))
}

func WithOutWriter(out io.Writer) LoggerOption {
	return LoggerOption(logger.WithOutWriter(out))
}

func WithErrWriter(err io.Writer) LoggerOption {
	return LoggerOption(logger.WithErrWriter(err))
}

func WithLogger(details trace.Details, opts ...LoggerOption) Option {
	return func(ctx context.Context, c *connection) error {
		nativeOpts := make([]logger.Option, 0, len(opts))
		for _, o := range opts {
			nativeOpts = append(nativeOpts, logger.Option(o))
		}
		l := logger.New(nativeOpts...)
		if err := WithTraceDriver(log.Driver(l, details))(ctx, c); err != nil {
			return err
		}
		return WithTraceTable(log.Table(l, details))(ctx, c)
	}
}

func WithConnectParams(params ConnectParams) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(
			c.options,
			config.WithEndpoint(params.Endpoint()),
			config.WithDatabase(params.Database()),
			config.WithSecure(params.Secure()),
		)
		if params.Token() != "" {
			c.options = append(
				c.options,
				config.WithCredentials(
					credentials.NewAccessTokenCredentials(
						params.Token(),
						credentials.WithSourceInfo("config.WithConnectParams()"),
					),
				),
			)
		}
		return nil
	}
}

func WithAnonymousCredentials() Option {
	return WithCredentials(
		credentials.NewAnonymousCredentials(credentials.WithSourceInfo("ydb.WithAnonymousCredentials()")),
	)
}

func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (credentials.Credentials, error)) Option {
	return func(ctx context.Context, c *connection) error {
		credentials, err := createCredentials(ctx)
		if err != nil {
			return err
		}
		c.options = append(c.options, config.WithCredentials(credentials))
		return nil
	}
}

func WithCredentials(c credentials.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return c, nil
	})
}

func WithBalancer(balancer ibalancer.Balancer) Option {
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

func MergeOptions(options ...Option) Option {
	return func(ctx context.Context, c *connection) error {
		for _, o := range options {
			if err := o(ctx, c); err != nil {
				return err
			}
		}
		return nil
	}
}

func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithDiscoveryInterval(discoveryInterval))
		return nil
	}
}

// WithTraceDriver returns deadline which has associated Driver with it.
func WithTraceDriver(trace trace.Driver) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, config.WithTrace(trace))
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
				return err
			}
			caFile = filepath.Join(home, caFile[1:])
		}
		bytes, err := ioutil.ReadFile(filepath.Clean(caFile))
		if err != nil {
			return err
		}
		return WithCertificatesFromPem(bytes)(ctx, c)
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
			return err
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

// WithTraceTable returns deadline which has associated Driver with it.
func WithTraceTable(trace trace.Table) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithTrace(trace))
		return nil
	}
}
