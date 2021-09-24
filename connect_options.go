package ydb

import (
	"context"
	"crypto/x509"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(ctx context.Context, client *db) error

type options struct {
	certPool                             *x509.CertPool
	connectTimeout                       *time.Duration
	traceDriver                          *trace.Driver
	traceTable                           *trace.Table
	driverConfig                         *config.Config
	credentials                          credentials.Credentials
	discoveryInterval                    *time.Duration
	tableSessionPoolSizeLimit            *int
	tableSessionPoolKeepAliveMinSize     *int
	tableSessionPoolIdleThreshold        *time.Duration
	tableSessionPoolKeepAliveTimeout     *time.Duration
	tableSessionPoolCreateSessionTimeout *time.Duration
	tableSessionPoolDeleteTimeout        *time.Duration
}

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		internal.NewAuthTokenCredentials(accessToken, "connect.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func NewAuthTokenCredentials(accessToken string) credentials.Credentials {
	return internal.NewAuthTokenCredentials(accessToken, "connect.NewAuthTokenCredentials(accessToken)") // hide access token for logs
}

func WithAnonymousCredentials() Option {
	return WithCredentials(
		internal.NewAnonymousCredentials("connect.WithAnonymousCredentials()"),
	)
}

func NewAnonymousCredentials() credentials.Credentials {
	return internal.NewAnonymousCredentials("connect.NewAnonymousCredentials()")
}

func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (credentials.Credentials, error)) Option {
	return func(ctx context.Context, c *db) error {
		credentials, err := createCredentials(ctx)
		if err != nil {
			return err
		}
		c.options.credentials = credentials
		return nil
	}
}

func WithCredentials(c credentials.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return c, nil
	})
}

func WithDriverConfig(config *config.Config) Option {
	return func(ctx context.Context, c *db) error {
		c.options.driverConfig = config
		return nil
	}
}

func WithGrpcConnectionPolicy(policy *config.GrpcConnectionPolicy) Option {
	return func(ctx context.Context, c *db) error {
		c.options.driverConfig.GrpcConnectionPolicy = policy
		return nil
	}
}

func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.discoveryInterval = &discoveryInterval
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolSizeLimit = &sizeLimit
		return nil
	}
}

func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolKeepAliveMinSize = &keepAliveMinSize
		return nil
	}
}

func WithSessionPoolIdleThreshold(idleThreshold time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolIdleThreshold = &idleThreshold
		return nil
	}
}

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolKeepAliveTimeout = &keepAliveTimeout
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolCreateSessionTimeout = &createSessionTimeout
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolDeleteTimeout = &deleteTimeout
		return nil
	}
}

// WithTraceDriver returns deadline which has associated Driver with it.
func WithTraceDriver(trace trace.Driver) Option {
	return func(ctx context.Context, c *db) error {
		c.options.traceDriver = &trace
		return nil
	}
}

// WithTraceTable returns deadline which has associated Driver with it.
func WithTraceTable(trace trace.Table) Option {
	return func(ctx context.Context, c *db) error {
		c.options.traceTable = &trace
		return nil
	}
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.connectTimeout = &connectTimeout
		return nil
	}
}

func WithCertificates(certPool *x509.CertPool) Option {
	return func(ctx context.Context, c *db) error {
		c.options.certPool = certPool
		return nil
	}
}

func WithCertificatesFromFile(caFile string) Option {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		panic(err)
	}
	err = credentials.AppendCertsFromFile(certPool, caFile)
	if err != nil {
		panic(err)
	}
	return WithCertificates(certPool)
}
