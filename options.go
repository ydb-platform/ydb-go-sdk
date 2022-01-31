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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(ctx context.Context, c *connection) error

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		ydb_credentials.NewAccessTokenCredentials(
			accessToken,
			ydb_credentials.WithSourceInfo(
				"ydb.WithAccessTokenCredentials(accessToken)", // hide access token for logs
			),
		),
	)
}

func WithUserAgent(userAgent string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithUserAgent(userAgent))
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
		c.options = append(c.options, ydb_config.WithConnectionTTL(ttl))
		return nil
	}
}

func WithEndpoint(endpoint string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithEndpoint(endpoint))
		return nil
	}
}

func WithDatabase(database string) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithDatabase(database))
		return nil
	}
}

func WithSecure(secure bool) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithSecure(secure))
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

func WithExternalLogger(external ydb_log.Logger) LoggerOption {
	return LoggerOption(logger.WithExternalLogger(external))
}

func WithOutWriter(out io.Writer) LoggerOption {
	return LoggerOption(logger.WithOutWriter(out))
}

func WithErrWriter(err io.Writer) LoggerOption {
	return LoggerOption(logger.WithErrWriter(err))
}

func WithLogger(details ydb_trace.Details, opts ...LoggerOption) Option {
	return func(ctx context.Context, c *connection) error {
		nativeOpts := make([]logger.Option, 0, len(opts))
		for _, o := range opts {
			nativeOpts = append(nativeOpts, logger.Option(o))
		}
		l := logger.New(nativeOpts...)
		if err := WithTraceDriver(ydb_log.Driver(l, details))(ctx, c); err != nil {
			return err
		}
		return WithTraceTable(ydb_log.Table(l, details))(ctx, c)
	}
}

func WithConnectParams(params ConnectParams) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(
			c.options,
			ydb_config.WithEndpoint(params.Endpoint()),
			ydb_config.WithDatabase(params.Database()),
			ydb_config.WithSecure(params.Secure()),
		)
		if params.Token() != "" {
			c.options = append(
				c.options,
				ydb_config.WithCredentials(
					ydb_credentials.NewAccessTokenCredentials(
						params.Token(),
						ydb_credentials.WithSourceInfo("ydb_config.WithConnectParams()"),
					),
				),
			)
		}
		return nil
	}
}

func WithAnonymousCredentials() Option {
	return WithCredentials(
		ydb_credentials.NewAnonymousCredentials(ydb_credentials.WithSourceInfo("ydb.WithAnonymousCredentials()")),
	)
}

func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (credentials.Credentials, error)) Option {
	return func(ctx context.Context, c *connection) error {
		credentials, err := createCredentials(ctx)
		if err != nil {
			return err
		}
		c.options = append(c.options, ydb_config.WithCredentials(credentials))
		return nil
	}
}

func WithCredentials(c credentials.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return c, nil
	})
}

func WithBalancer(balancer balancer.Balancer) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithBalancer(balancer))
		return nil
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithDialTimeout(timeout))
		return nil
	}
}

func With(options ...ydb_config.Option) Option {
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
		c.options = append(c.options, ydb_config.WithDiscoveryInterval(discoveryInterval))
		return nil
	}
}

// WithTraceDriver returns deadline which has associated Driver with it.
func WithTraceDriver(trace ydb_trace.Driver) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithTrace(trace))
		return nil
	}
}

func WithCertificate(cert *x509.Certificate) Option {
	return func(ctx context.Context, c *connection) error {
		c.options = append(c.options, ydb_config.WithCertificate(cert))
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

func WithTableConfigOption(option ydb_table_config.Option) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, option)
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, ydb_table_config.WithSizeLimit(sizeLimit))
		return nil
	}
}

func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, ydb_table_config.WithKeepAliveMinSize(keepAliveMinSize))
		return nil
	}
}

func WithSessionPoolIdleThreshold(idleThreshold time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, ydb_table_config.WithIdleThreshold(idleThreshold))
		return nil
	}
}

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, ydb_table_config.WithKeepAliveTimeout(keepAliveTimeout))
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, ydb_table_config.WithCreateSessionTimeout(createSessionTimeout))
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, ydb_table_config.WithDeleteTimeout(deleteTimeout))
		return nil
	}
}

// WithTraceTable returns deadline which has associated Driver with it.
func WithTraceTable(trace ydb_trace.Table) Option {
	return func(ctx context.Context, c *connection) error {
		c.tableOptions = append(c.tableOptions, ydb_table_config.WithTrace(trace))
		return nil
	}
}
