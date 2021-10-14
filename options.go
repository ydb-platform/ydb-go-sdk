package ydb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os/user"
	"path/filepath"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(ctx context.Context, client *db) error

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		internal.NewAccessTokenCredentials(accessToken, "ydb.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func WithConnectionString(connection string) Option {
	return func(ctx context.Context, c *db) error {
		params, err := ConnectionString(connection)
		if err != nil {
			return err
		}
		return WithConnectParams(params)(ctx, c)
	}
}

func WithConnectParams(params ConnectParams) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, config.WithEndpoint(params.Endpoint()))
		c.options = append(c.options, config.WithDatabase(params.Database()))
		if params.Token() != "" {
			c.options = append(c.options, config.WithCredentials(internal.NewAccessTokenCredentials(params.Token(), "config.WithConnectParams()")))
		}
		if params.UseTLS() {
			c.options = append(c.options, config.WithTLSConfig(&tls.Config{}))
		} else {
			c.options = append(c.options, config.WithTLSConfig(nil))
		}
		return nil
	}
}

func WithAnonymousCredentials() Option {
	return WithCredentials(
		internal.NewAnonymousCredentials("ydb.WithAnonymousCredentials()"),
	)
}

func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (credentials.Credentials, error)) Option {
	return func(ctx context.Context, c *db) error {
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

func WithDriverConfigOptions(options ...config.Option) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, options...)
		return nil
	}
}

func WithBalancingConfig(balancerConfig config.BalancerConfig) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, config.WithBalancingConfig(balancerConfig))
		return nil
	}
}

func WithGrpcConnectionTTL(ttl time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		// TODO: sync with table session keep-alive timeout
		c.options = append(c.options, config.WithGrpcConnectionTTL(ttl))
		return nil
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, config.WithDialTimeout(timeout))
		return nil
	}
}

func With(options ...config.Option) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, options...)
		return nil
	}
}

func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, config.WithDiscoveryInterval(discoveryInterval))
		return nil
	}
}

// WithTraceDriver returns deadline which has associated Driver with it.
func WithTraceDriver(trace trace.Driver) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, config.WithTrace(trace))
		return nil
	}
}

func WithCertificate(cert *x509.Certificate) Option {
	return func(ctx context.Context, c *db) error {
		c.options = append(c.options, config.WithCertificate(cert))
		return nil
	}
}

func WithCertificatesFromFile(caFile string) Option {
	return func(ctx context.Context, c *db) error {
		if len(caFile) > 0 || caFile[0] == '~' {
			usr, err := user.Current()
			if err != nil {
				return err
			}
			caFile = filepath.Join(usr.HomeDir, caFile[1:])
		}
		bytes, err := ioutil.ReadFile(caFile)
		if err != nil {
			return err
		}
		return WithCertificatesFromPem(bytes)(ctx, c)
	}
}

func WithCertificatesFromPem(bytes []byte) Option {
	return func(ctx context.Context, c *db) error {
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
