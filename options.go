package ydb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/certificates"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	coordinationConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	ratelimiterConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/config"
	schemeConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	scriptingConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting/config"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Option contains configuration values for Driver
type Option func(ctx context.Context, d *Driver) error

func WithStaticCredentials(user, password string) Option {
	return func(ctx context.Context, c *Driver) error {
		c.userInfo = &dsn.UserInfo{
			User:     user,
			Password: password,
		}

		return nil
	}
}

// WithNodeAddressMutator applies mutator for node addresses from discovery.ListEndpoints response
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithNodeAddressMutator(mutator func(address string) string) Option {
	return func(ctx context.Context, c *Driver) error {
		c.discoveryOptions = append(c.discoveryOptions, discoveryConfig.WithAddressMutator(mutator))

		return nil
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

// WithOauth2TokenExchangeCredentials adds credentials that exchange token using
// OAuth 2.0 token exchange protocol:
// https://www.rfc-editor.org/rfc/rfc8693
func WithOauth2TokenExchangeCredentials(
	opts ...credentials.Oauth2TokenExchangeCredentialsOption,
) Option {
	opts = append(opts, credentials.WithSourceInfo("ydb.WithOauth2TokenExchangeCredentials(opts)"))

	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return credentials.NewOauth2TokenExchangeCredentials(opts...)
	})
}

/*
WithOauth2TokenExchangeCredentialsFile adds credentials that exchange token using
OAuth 2.0 token exchange protocol:
https://www.rfc-editor.org/rfc/rfc8693
Config file must be a valid json file

Fields of json file

	grant-type:           [string] Grant type option (default: "urn:ietf:params:oauth:grant-type:token-exchange")
	res:                  [string] Resource option (optional)
	aud:                  [string | list of strings] Audience option for token exchange request (optional)
	scope:                [string | list of strings] Scope option (optional)
	requested-token-type: [string] Requested token type option (default: "urn:ietf:params:oauth:token-type:access_token")
	subject-credentials:  [creds_json] Subject credentials options (optional)
	actor-credentials:    [creds_json] Actor credentials options (optional)
	token-endpoint:       [string] Token endpoint

Fields of creds_json (JWT):

	type:                 [string] Token source type. Set JWT
	alg:                  [string] Algorithm for JWT signature.
								   Supported algorithms can be listed
								   with GetSupportedOauth2TokenExchangeJwtAlgorithms()
	private-key:          [string] (Private) key in PEM format (RSA, EC) or Base64 format (HMAC) for JWT signature
	kid:                  [string] Key id JWT standard claim (optional)
	iss:                  [string] Issuer JWT standard claim (optional)
	sub:                  [string] Subject JWT standard claim (optional)
	aud:                  [string | list of strings] Audience JWT standard claim (optional)
	jti:                  [string] JWT ID JWT standard claim (optional)
	ttl:                  [string] Token TTL (default: 1h)

Fields of creds_json (FIXED):

	type:                 [string] Token source type. Set FIXED
	token:                [string] Token value
	token-type:           [string] Token type value. It will become
								   subject_token_type/actor_token_type parameter
								   in token exchange request (https://www.rfc-editor.org/rfc/rfc8693)
*/
func WithOauth2TokenExchangeCredentialsFile(
	configFilePath string,
	opts ...credentials.Oauth2TokenExchangeCredentialsOption,
) Option {
	srcInfo := credentials.WithSourceInfo(fmt.Sprintf("ydb.WithOauth2TokenExchangeCredentialsFile(%s)", configFilePath))
	opts = append(opts, srcInfo)

	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return credentials.NewOauth2TokenExchangeCredentialsFile(configFilePath, opts...)
	})
}

// WithApplicationName add provided application name to all api requests
func WithApplicationName(applicationName string) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithApplicationName(applicationName))

		return nil
	}
}

// WithUserAgent add provided user agent value to all api requests
//
// Deprecated: use WithApplicationName instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithUserAgent(userAgent string) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithApplicationName(userAgent))

		return nil
	}
}

func WithRequestsType(requestsType string) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithRequestsType(requestsType))

		return nil
	}
}

// WithConnectionString accept Driver string like
//
//	grpc[s]://{endpoint}/{database}[?param=value]
//
// Warning: WithConnectionString will be removed at next major release
//
// (Driver string will be required string param of ydb.Open)
func WithConnectionString(connectionString string) Option {
	return func(ctx context.Context, c *Driver) error {
		if connectionString == "" {
			return nil
		}
		info, err := dsn.Parse(connectionString)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("parse connection string '%s' failed: %w", connectionString, err),
			)
		}
		c.options = append(c.options, info.Options...)
		c.userInfo = info.UserInfo

		return nil
	}
}

// WithConnectionTTL defines duration for parking idle connections
func WithConnectionTTL(ttl time.Duration) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithConnectionTTL(ttl))

		return nil
	}
}

// WithEndpoint defines endpoint option
//
// Warning: use ydb.Open with required Driver string parameter instead
//
// For making Driver string from endpoint+database+secure - use sugar.DSN()
func WithEndpoint(endpoint string) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithEndpoint(endpoint))

		return nil
	}
}

// WithDatabase defines database option
//
// Warning: use ydb.Open with required Driver string parameter instead
//
// For making Driver string from endpoint+database+secure - use sugar.DSN()
func WithDatabase(database string) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithDatabase(database))

		return nil
	}
}

// WithSecure defines secure option
//
// Warning: use ydb.Open with required Driver string parameter instead
//
// For making Driver string from endpoint+database+secure - use sugar.DSN()
func WithSecure(secure bool) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithSecure(secure))

		return nil
	}
}

// WithInsecure defines secure option.
//
// Warning: WithInsecure lost current TLS config.
func WithInsecure() Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithSecure(false))

		return nil
	}
}

// WithMinTLSVersion set minimum TLS version acceptable for connections
func WithMinTLSVersion(minVersion uint16) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithMinTLSVersion(minVersion))

		return nil
	}
}

// WithTLSSInsecureSkipVerify applies InsecureSkipVerify flag to TLS config
func WithTLSSInsecureSkipVerify() Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithTLSSInsecureSkipVerify())

		return nil
	}
}

// WithLogger add enables logging for selected tracing events.
//
// See trace package documentation for details.
func WithLogger(l log.Logger, details trace.Detailer, opts ...log.Option) Option {
	return func(ctx context.Context, c *Driver) error {
		c.logger = l
		c.loggerOpts = opts
		c.loggerDetails = details

		return nil
	}
}

// WithAnonymousCredentials force to make requests withou authentication.
func WithAnonymousCredentials() Option {
	return WithCredentials(
		credentials.NewAnonymousCredentials(credentials.WithSourceInfo("ydb.WithAnonymousCredentials()")),
	)
}

// WithCreateCredentialsFunc add callback funcion to provide requests credentials
func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (credentials.Credentials, error)) Option {
	return func(ctx context.Context, c *Driver) error {
		creds, err := createCredentials(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		c.options = append(c.options, config.WithCredentials(creds))

		return nil
	}
}

// WithCredentials in conjunction with Driver.With function prohibit reuse of conn pool.
// Thus, Driver.With will effectively create totally separate Driver.
func WithCredentials(c credentials.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (credentials.Credentials, error) {
		return c, nil
	})
}

func WithBalancer(balancer *balancerConfig.Config) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithBalancer(balancer))

		return nil
	}
}

// WithDialTimeout sets timeout for establishing new Driver to cluster
//
// Default dial timeout is config.DefaultDialTimeout
func WithDialTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithDialTimeout(timeout))

		return nil
	}
}

// With collects additional configuration options.
//
// This option does not replace collected option, instead it will append provided options.
func With(options ...config.Option) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, options...)

		return nil
	}
}

// MergeOptions concatentaes provided options to one cumulative value.
func MergeOptions(opts ...Option) Option {
	return func(ctx context.Context, c *Driver) error {
		for _, opt := range opts {
			if opt != nil {
				if err := opt(ctx, c); err != nil {
					return xerrors.WithStackTrace(err)
				}
			}
		}

		return nil
	}
}

// WithDiscoveryInterval sets interval between cluster discovery calls.
func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, c *Driver) error {
		c.discoveryOptions = append(c.discoveryOptions, discoveryConfig.WithInterval(discoveryInterval))

		return nil
	}
}

// WithRetryBudget sets retry budget for all calls of all retryers.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithRetryBudget(b budget.Budget) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithRetryBudget(b))

		return nil
	}
}

// WithTraceDriver appends trace.Driver into driver traces
func WithTraceDriver(t trace.Driver, opts ...trace.DriverComposeOption) Option { //nolint:gocritic
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithTrace(t, opts...))

		return nil
	}
}

// WithTraceRetry appends trace.Retry into retry traces
func WithTraceRetry(t trace.Retry, opts ...trace.RetryComposeOption) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options,
			config.WithTraceRetry(&t, append(
				[]trace.RetryComposeOption{
					trace.WithRetryPanicCallback(c.panicCallback),
				},
				opts...,
			)...),
		)

		return nil
	}
}

// WithCertificate appends certificate to TLS config root certificates
func WithCertificate(cert *x509.Certificate) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithCertificate(cert))

		return nil
	}
}

// WithCertificatesFromFile appends certificates by filepath to TLS config root certificates
func WithCertificatesFromFile(caFile string, opts ...certificates.FromFileOption) Option {
	if len(caFile) > 0 && caFile[0] == '~' {
		if home, err := os.UserHomeDir(); err == nil {
			caFile = filepath.Join(home, caFile[1:])
		}
	}
	if file, err := filepath.Abs(caFile); err == nil {
		caFile = file
	}
	if file, err := filepath.EvalSymlinks(caFile); err == nil {
		caFile = file
	}

	return func(ctx context.Context, c *Driver) error {
		certs, err := certificates.FromFile(caFile, opts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		for _, cert := range certs {
			if err := WithCertificate(cert)(ctx, c); err != nil {
				return xerrors.WithStackTrace(err)
			}
		}

		return nil
	}
}

// WithTLSConfig replaces older TLS config
//
// Warning: all early TLS config changes (such as WithCertificate, WithCertificatesFromFile, WithCertificatesFromPem,
// WithMinTLSVersion, WithTLSSInsecureSkipVerify) will be lost
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(ctx context.Context, c *Driver) error {
		c.options = append(c.options, config.WithTLSConfig(tlsConfig))

		return nil
	}
}

// WithCertificatesFromPem appends certificates from pem-encoded data to TLS config root certificates
func WithCertificatesFromPem(bytes []byte, opts ...certificates.FromPemOption) Option {
	return func(ctx context.Context, c *Driver) error {
		certs, err := certificates.FromPem(bytes, opts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		for _, cert := range certs {
			_ = WithCertificate(cert)(ctx, c)
		}

		return nil
	}
}

// WithTableConfigOption collects additional configuration options for table.Client.
// This option does not replace collected option, instead it will appen provided options.
func WithTableConfigOption(option tableConfig.Option) Option {
	return func(ctx context.Context, c *Driver) error {
		c.tableOptions = append(c.tableOptions, option)

		return nil
	}
}

// WithQueryConfigOption collects additional configuration options for query.Client.
// This option does not replace collected option, instead it will appen provided options.
func WithQueryConfigOption(option queryConfig.Option) Option {
	return func(ctx context.Context, c *Driver) error {
		c.queryOptions = append(c.queryOptions, option)

		return nil
	}
}

// WithSessionPoolSizeLimit set max size of internal sessions pool in table.Client
func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *Driver) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithSizeLimit(sizeLimit))
		c.queryOptions = append(c.queryOptions, queryConfig.WithPoolLimit(sizeLimit))

		return nil
	}
}

// WithSessionPoolIdleThreshold defines interval for idle sessions
func WithSessionPoolIdleThreshold(idleThreshold time.Duration) Option {
	return func(ctx context.Context, c *Driver) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithIdleThreshold(idleThreshold))
		c.databaseSQLOptions = append(
			c.databaseSQLOptions,
			xsql.WithIdleThreshold(idleThreshold),
		)

		return nil
	}
}

// WithSessionPoolCreateSessionTimeout set timeout for new session creation process in table.Client
func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *Driver) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithCreateSessionTimeout(createSessionTimeout))
		c.queryOptions = append(c.queryOptions, queryConfig.WithSessionCreateTimeout(createSessionTimeout))

		return nil
	}
}

// WithSessionPoolDeleteTimeout set timeout to gracefully close deleting session in table.Client
func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *Driver) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithDeleteTimeout(deleteTimeout))
		c.queryOptions = append(c.queryOptions, queryConfig.WithSessionDeleteTimeout(deleteTimeout))

		return nil
	}
}

// WithSessionPoolKeepAliveMinSize set minimum sessions should be keeped alive in table.Client
//
// Deprecated: use WithApplicationName instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(ctx context.Context, c *Driver) error { return nil }
}

// WithSessionPoolKeepAliveTimeout set timeout of keep alive requests for session in table.Client
//
// Deprecated: use WithApplicationName instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *Driver) error { return nil }
}

// WithIgnoreTruncated disables errors on truncated flag
func WithIgnoreTruncated() Option {
	return func(ctx context.Context, c *Driver) error {
		c.tableOptions = append(c.tableOptions, tableConfig.WithIgnoreTruncated())

		return nil
	}
}

// WithPanicCallback specified behavior on panic
// Warning: WithPanicCallback must be defined on start of all options
// (before `WithTrace{Driver,Table,Scheme,Scripting,Coordination,Ratelimiter}` and other options)
// If not defined - panic would not intercept with driver
func WithPanicCallback(panicCallback func(e interface{})) Option {
	return func(ctx context.Context, c *Driver) error {
		c.panicCallback = panicCallback
		c.options = append(c.options, config.WithPanicCallback(panicCallback))

		return nil
	}
}

// WithTraceTable appends trace.Table into table traces
func WithTraceTable(t trace.Table, opts ...trace.TableComposeOption) Option { //nolint:gocritic
	return func(ctx context.Context, c *Driver) error {
		c.tableOptions = append(
			c.tableOptions,
			tableConfig.WithTrace(
				&t,
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

// WithTraceQuery appends trace.Query into query traces
func WithTraceQuery(t trace.Query, opts ...trace.QueryComposeOption) Option { //nolint:gocritic
	return func(ctx context.Context, c *Driver) error {
		c.queryOptions = append(
			c.queryOptions,
			queryConfig.WithTrace(&t,
				append(
					[]trace.QueryComposeOption{
						trace.WithQueryPanicCallback(c.panicCallback),
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
	return func(ctx context.Context, c *Driver) error {
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
	return func(ctx context.Context, c *Driver) error {
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
func WithTraceCoordination(t trace.Coordination, opts ...trace.CoordinationComposeOption) Option { //nolint:gocritic
	return func(ctx context.Context, c *Driver) error {
		c.coordinationOptions = append(
			c.coordinationOptions,
			coordinationConfig.WithTrace(
				&t,
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
	return func(ctx context.Context, c *Driver) error {
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
	return func(ctx context.Context, c *Driver) error {
		c.ratelimiterOptions = append(c.ratelimiterOptions, opts...)

		return nil
	}
}

// WithTraceDiscovery adds configured discovery tracer to Driver
func WithTraceDiscovery(t trace.Discovery, opts ...trace.DiscoveryComposeOption) Option {
	return func(ctx context.Context, c *Driver) error {
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

// WithTraceTopic adds configured discovery tracer to Driver
func WithTraceTopic(t trace.Topic, opts ...trace.TopicComposeOption) Option { //nolint:gocritic
	return func(ctx context.Context, c *Driver) error {
		c.topicOptions = append(
			c.topicOptions,
			topicoptions.WithTrace(
				t,
				append(
					[]trace.TopicComposeOption{
						trace.WithTopicPanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)

		return nil
	}
}

// WithTraceDatabaseSQL adds configured discovery tracer to Driver
func WithTraceDatabaseSQL(t trace.DatabaseSQL, opts ...trace.DatabaseSQLComposeOption) Option { //nolint:gocritic
	return func(ctx context.Context, c *Driver) error {
		c.databaseSQLOptions = append(
			c.databaseSQLOptions,
			xsql.WithTrace(
				&t,
				append(
					[]trace.DatabaseSQLComposeOption{
						trace.WithDatabaseSQLPanicCallback(c.panicCallback),
					},
					opts...,
				)...,
			),
		)

		return nil
	}
}

// Private technical options for correct copies processing

func withOnClose(onClose func(c *Driver)) Option {
	return func(ctx context.Context, c *Driver) error {
		c.onClose = append(c.onClose, onClose)

		return nil
	}
}

func withConnPool(pool *conn.Pool) Option {
	return func(ctx context.Context, c *Driver) error {
		c.pool = pool

		return pool.Take(ctx)
	}
}
