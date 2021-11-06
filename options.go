package ydb

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(ctx context.Context, db *db) error

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		internal.NewAccessTokenCredentials(accessToken, "ydb.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func WithConnectionString(connection string) Option {
	return func(ctx context.Context, db *db) error {
		params, err := ConnectionString(connection)
		if err != nil {
			return err
		}
		return WithConnectParams(params)(ctx, db)
	}
}

func WithEndpoint(endpoint string) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithEndpoint(endpoint))
		return nil
	}
}

func WithDatabase(database string) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithDatabase(database))
		return nil
	}
}

func WithNamespace(namespace string) logger.Option {
	return logger.WithNamespace(namespace)
}

func WithMinLevel(minLevel logger.Level) logger.Option {
	return logger.WithMinLevel(minLevel)
}

func WithNoColor(b bool) logger.Option {
	return logger.WithNoColor(b)
}

func WithLogger(details trace.Details, opts ...logger.Option) Option {
	return func(ctx context.Context, db *db) error {
		l := logger.New(opts...)
		if err := WithTraceDriver(log.Driver(l, details))(ctx, db); err != nil {
			return err
		}
		return WithTraceTable(log.Table(l, details))(ctx, db)
	}
}

func WithConnectParams(params ConnectParams) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithEndpoint(params.Endpoint()))
		db.options = append(db.options, config.WithDatabase(params.Database()))
		db.options = append(db.options, config.WithSecure(params.Secure()))
		if params.Token() != "" {
			db.options = append(db.options, config.WithCredentials(internal.NewAccessTokenCredentials(params.Token(), "config.WithConnectParams()")))
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
	return func(ctx context.Context, db *db) error {
		credentials, err := createCredentials(ctx)
		if err != nil {
			return err
		}
		db.options = append(db.options, config.WithCredentials(credentials))
		return nil
	}
}

func WithCredentials(c credentials.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return c, nil
	})
}

func WithDriverConfigOptions(options ...config.Option) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, options...)
		return nil
	}
}

func WithBalancingConfig(balancerConfig config.BalancerConfig) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithBalancingConfig(balancerConfig))
		return nil
	}
}

func WithGrpcConnectionTTL(ttl time.Duration) Option {
	return func(ctx context.Context, db *db) error {
		// TODO: sync with table session keep-alive timeout
		db.options = append(db.options, config.WithGrpcConnectionTTL(ttl))
		return nil
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithDialTimeout(timeout))
		return nil
	}
}

func With(options ...config.Option) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, options...)
		return nil
	}
}

func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithDiscoveryInterval(discoveryInterval))
		return nil
	}
}

// WithTraceDriver returns deadline which has associated Driver with it.
func WithTraceDriver(trace trace.Driver) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithTrace(trace))
		return nil
	}
}

func WithCertificate(cert *x509.Certificate) Option {
	return func(ctx context.Context, db *db) error {
		db.options = append(db.options, config.WithCertificate(cert))
		return nil
	}
}

func WithCertificatesFromFile(caFile string) Option {
	return func(ctx context.Context, db *db) error {
		if len(caFile) > 0 && caFile[0] == '~' {
			usr, err := user.Current()
			if err != nil {
				return err
			}
			caFile = filepath.Join(usr.HomeDir, caFile[1:])
		}
		bytes, err := os.ReadFile(caFile)
		if err != nil {
			return err
		}
		return WithCertificatesFromPem(bytes)(ctx, db)
	}
}

func WithCertificatesFromPem(bytes []byte) Option {
	return func(ctx context.Context, db *db) error {
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
				_ = WithCertificate(cert)(ctx, db)
				ok = true
			}
			return
		}(bytes); !ok {
			return err
		}
		return nil
	}
}
