package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"time"

	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"

	routerconfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/router/config"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/xnet"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xresolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Config contains driver configuration.
type Config struct {
	config.Common

	trace         trace.Driver
	dialTimeout   time.Duration
	connectionTTL time.Duration
	routerConfig  *routerconfig.Config
	secure        bool
	endpoint      string
	database      string
	requestsType  string
	userAgent     string
	grpcOptions   []grpc.DialOption
	credentials   credentials.Credentials
	tlsConfig     *tls.Config
	meta          meta.Meta

	excludeGRPCCodesForPessimization []grpcCodes.Code
}

// ExcludeGRPCCodesForPessimization defines grpc codes for exclude its from pessimization trigger
func (c Config) ExcludeGRPCCodesForPessimization() []grpcCodes.Code {
	return c.excludeGRPCCodesForPessimization
}

// GrpcDialOptions reports about used grpc dialing options
func (c Config) GrpcDialOptions() []grpc.DialOption {
	return c.grpcOptions
}

// Meta reports meta information about database connection
func (c Config) Meta() meta.Meta {
	return c.meta
}

// ConnectionTTL defines interval for parking grpc connections.
//
// If ConnectionTTL is zero - connections are not park.
func (c Config) ConnectionTTL() time.Duration {
	return c.connectionTTL
}

// Secure is a flag for secure connection
func (c Config) Secure() bool {
	return c.secure
}

// Endpoint is a required starting endpoint for connect
func (c Config) Endpoint() string {
	return c.endpoint
}

// TLSConfig reports about TLS configuration
func (c Config) TLSConfig() *tls.Config {
	return c.tlsConfig
}

// DialTimeout is the maximum amount of time a dial will wait for a connect to
// complete.
//
// If DialTimeout is zero then no timeout is used.
func (c Config) DialTimeout() time.Duration {
	return c.dialTimeout
}

// Database is a required database name.
func (c Config) Database() string {
	return c.database
}

// Credentials is a ydb client credentials.
// In most cases Credentials are required.
func (c Config) Credentials() credentials.Credentials {
	return c.credentials
}

// Trace contains driver tracing options.
func (c Config) Trace() trace.Driver {
	return c.trace
}

// Balancer is an optional configuration related to selected balancer.
// That is, some balancing methods allow to be configured.
func (c Config) Balancer() *routerconfig.Config {
	return c.routerConfig
}

// RequestsType set an additional type hint to all requests.
// It is needed only for debug purposes and advanced cases.
func (c Config) RequestsType() string {
	return c.requestsType
}

type Option func(c *Config)

// WithInternalDNSResolver
//
// Deprecated: always used internal dns-resolver
func WithInternalDNSResolver() Option {
	return func(c *Config) {}
}

func WithEndpoint(endpoint string) Option {
	return func(c *Config) {
		c.endpoint = endpoint
	}
}

// WithSecure changes secure connection flag.
//
// Warning: if secure is false - TLS config options has no effect.
func WithSecure(secure bool) Option {
	return func(c *Config) {
		c.secure = secure
	}
}

func WithDatabase(database string) Option {
	return func(c *Config) {
		c.database = database
	}
}

// WithCertificate appends certificate to TLS config root certificates
func WithCertificate(certificate *x509.Certificate) Option {
	return func(c *Config) {
		c.tlsConfig.RootCAs.AddCert(certificate)
	}
}

// WithTLSConfig replaces older TLS config
//
// Warning: all early changes of TLS config will be lost
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(c *Config) {
		c.tlsConfig = tlsConfig
	}
}

func WithTrace(t trace.Driver, opts ...trace.DriverComposeOption) Option {
	return func(c *Config) {
		c.trace = c.trace.Compose(t, opts...)
	}
}

func WithUserAgent(userAgent string) Option {
	return func(c *Config) {
		c.userAgent = userAgent
	}
}

func WithConnectionTTL(ttl time.Duration) Option {
	return func(c *Config) {
		c.connectionTTL = ttl
	}
}

func WithCredentials(credentials credentials.Credentials) Option {
	return func(c *Config) {
		c.credentials = credentials
	}
}

// WithOperationTimeout defines the maximum amount of time a YDB server will process
// an operation. After timeout exceeds YDB will try to cancel operation and
// regardless of the cancellation appropriate error will be returned to
// the client.
//
// If OperationTimeout is zero then no timeout is used.
func WithOperationTimeout(operationTimeout time.Duration) Option {
	return func(c *Config) {
		config.SetOperationTimeout(&c.Common, operationTimeout)
	}
}

// WithOperationCancelAfter sets the maximum amount of time a YDB server will process an
// operation. After timeout exceeds YDB will try to cancel operation and if
// it succeeds appropriate error will be returned to the client; otherwise
// processing will be continued.
//
// If OperationCancelAfter is zero then no timeout is used.
func WithOperationCancelAfter(operationCancelAfter time.Duration) Option {
	return func(c *Config) {
		config.SetOperationCancelAfter(&c.Common, operationCancelAfter)
	}
}

// WithNoAutoRetry disable auto-retry calls from YDB sub-clients
func WithNoAutoRetry() Option {
	return func(c *Config) {
		config.SetAutoRetry(&c.Common, false)
	}
}

// WithPanicCallback applies panic callback to config
func WithPanicCallback(panicCallback func(e interface{})) Option {
	return func(c *Config) {
		config.SetPanicCallback(&c.Common, panicCallback)
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.dialTimeout = timeout
	}
}

func WithBalancer(balancer *routerconfig.Config) Option {
	return func(c *Config) {
		c.routerConfig = balancer
	}
}

func WithRequestsType(requestsType string) Option {
	return func(c *Config) {
		c.requestsType = requestsType
	}
}

// WithMinTLSVersion applies minimum TLS version that is acceptable.
func WithMinTLSVersion(minVersion uint16) Option {
	return func(c *Config) {
		c.tlsConfig.MinVersion = minVersion
	}
}

// WithTLSSInsecureSkipVerify applies InsecureSkipVerify flag to TLS config
func WithTLSSInsecureSkipVerify() Option {
	return func(c *Config) {
		c.tlsConfig.InsecureSkipVerify = true
	}
}

func WithGrpcOptions(option ...grpc.DialOption) Option {
	return func(c *Config) {
		c.grpcOptions = append(c.grpcOptions, option...)
	}
}

func ExcludeGRPCCodesForPessimization(codes ...grpcCodes.Code) Option {
	return func(c *Config) {
		c.excludeGRPCCodesForPessimization = append(
			c.excludeGRPCCodesForPessimization,
			codes...,
		)
	}
}

func New(opts ...Option) Config {
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}
	c.grpcOptions = append(
		c.grpcOptions,
		grpcCredentials(
			c.secure,
			c.tlsConfig,
		),
	)
	c.meta = meta.New(
		c.database,
		c.credentials,
		c.trace,
		c.requestsType,
		c.userAgent,
	)
	return c
}

func certPool() *x509.CertPool {
	certPool, err := x509.SystemCertPool()
	if err == nil {
		return certPool
	}
	return x509.NewCertPool()
}

func defaultTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    certPool(),
	}
}

func defaultConfig() (c Config) {
	return Config{
		credentials: credentials.NewAnonymousCredentials(
			credentials.WithSourceInfo("default"),
		),
		routerConfig: balancers.Default(),
		tlsConfig:    defaultTLSConfig(),
		grpcOptions: []grpc.DialOption{
			grpc.WithContextDialer(
				func(ctx context.Context, address string) (net.Conn, error) {
					return builder.New(
						ctx,
						address,
						c.trace,
					)
				},
			),
			grpc.WithKeepaliveParams(
				DefaultGrpcConnectionPolicy,
			),
			grpc.WithDefaultServiceConfig(`{
				"loadBalancingPolicy": "round_robin"
			}`),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(DefaultGRPCMsgSize),
				grpc.MaxCallSendMsgSize(DefaultGRPCMsgSize),
			),
			grpc.WithResolvers(
				xresolver.New("", c.trace),
				xresolver.New("ydb", c.trace),
				xresolver.New("grpc", c.trace),
				xresolver.New("grpcs", c.trace),
			),
		},
	}
}
