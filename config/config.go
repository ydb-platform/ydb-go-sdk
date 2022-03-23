package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/net"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/resolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Config contains driver configuration options.
type Config interface {
	// Endpoint is a required starting endpoint for connect
	Endpoint() string

	// Database is a required database name.
	Database() string

	// Secure is an flag for secure connection
	Secure() bool

	// Credentials is an ydb client credentials.
	// In most cases Credentials are required.
	Credentials() credentials.Credentials

	// Trace contains driver tracing options.
	Trace() trace.Driver

	// OperationTimeout is the maximum amount of time a YDB server will process
	// an operation. After timeout exceeds YDB will try to cancel operation and
	// regardless of the cancellation appropriate error will be returned to
	// the client.
	// If OperationTimeout is zero then no timeout is used.
	OperationTimeout() time.Duration

	// OperationCancelAfter is the maximum amount of time a YDB server will process an
	// operation. After timeout exceeds YDB will try to cancel operation and if
	// it succeeds appropriate error will be returned to the client; otherwise
	// processing will be continued.
	// If OperationCancelAfter is zero then no timeout is used.
	OperationCancelAfter() time.Duration

	// Balancer is an optional configuration related to selected balancer.
	// That is, some balancing methods allow to be configured.
	Balancer() balancer.Balancer

	// RequestsType set an additional type hint to all requests.
	// It is needed only for debug purposes and advanced cases.
	RequestsType() string

	// DialTimeout is the maximum amount of time a dial will wait for a connect to
	// complete.
	// If DialTimeout is zero then no timeout is used.
	DialTimeout() time.Duration

	// GrpcDialOptions is an custom client grpc dial options which will appends to
	// default grpc dial options
	GrpcDialOptions() []grpc.DialOption

	// ConnectionTTL is a time to live of a connection
	// If ConnectionTTL is zero then TTL is not used.
	ConnectionTTL() time.Duration

	// Meta is an option which contains meta information about database connection
	Meta() meta.Meta

	// UseDNSResolver is a flag about using dns-resolving or not
	UseDNSResolver() bool

	// ExcludeGRPCCodesForPessimization defines grpc codes for exclude its from pessimization trigger
	ExcludeGRPCCodesForPessimization() []errors.TransportErrorCode
}

// Config contains driver configuration options.
type config struct {
	trace                            trace.Driver
	operationTimeout                 time.Duration
	operationCancelAfter             time.Duration
	dialTimeout                      time.Duration
	connectionTTL                    time.Duration
	balancer                         balancer.Balancer
	secure                           bool
	dnsResolver                      bool
	endpoint                         string
	database                         string
	requestsType                     string
	userAgent                        string
	excludeGRPCCodesForPessimization []errors.TransportErrorCode
	grpcOptions                      []grpc.DialOption
	credentials                      credentials.Credentials
	tlsConfig                        *tls.Config
	meta                             meta.Meta
}

func (c *config) ExcludeGRPCCodesForPessimization() []errors.TransportErrorCode {
	return c.excludeGRPCCodesForPessimization
}

func (c *config) UseDNSResolver() bool {
	return c.dnsResolver
}

func (c *config) GrpcDialOptions() []grpc.DialOption {
	return c.grpcOptions
}

func (c *config) Meta() meta.Meta {
	return c.meta
}

func (c *config) ConnectionTTL() time.Duration {
	return c.connectionTTL
}

func (c *config) Secure() bool {
	return c.secure
}

func (c *config) Endpoint() string {
	return c.endpoint
}

func (c *config) TLSConfig() *tls.Config {
	return c.tlsConfig
}

func (c *config) DialTimeout() time.Duration {
	return c.dialTimeout
}

func (c *config) Database() string {
	return c.database
}

func (c *config) Credentials() credentials.Credentials {
	return c.credentials
}

func (c *config) Trace() trace.Driver {
	return c.trace
}

func (c *config) OperationTimeout() time.Duration {
	return c.operationTimeout
}

func (c *config) OperationCancelAfter() time.Duration {
	return c.operationCancelAfter
}

func (c *config) Balancer() balancer.Balancer {
	return c.balancer
}

func (c *config) RequestsType() string {
	return c.requestsType
}

type Option func(c *config)

// WithInternalDNSResolver disable dns-resolving before dialing
// If dns-resolving are disabled - dial used FQDN as address
// If dns-resolving are enabled - dial used IP-address
func WithInternalDNSResolver() Option {
	return func(c *config) {
		c.dnsResolver = true
	}
}

func WithEndpoint(endpoint string) Option {
	return func(c *config) {
		c.endpoint = endpoint
	}
}

func WithSecure(secure bool) Option {
	return func(c *config) {
		c.secure = secure
	}
}

func WithDatabase(database string) Option {
	return func(c *config) {
		c.database = database
	}
}

func WithCertificate(certificate *x509.Certificate) Option {
	return func(c *config) {
		c.tlsConfig.RootCAs.AddCert(certificate)
	}
}

func WithTrace(trace trace.Driver) Option {
	return func(c *config) {
		c.trace = c.trace.Compose(trace)
	}
}

func WithUserAgent(userAgent string) Option {
	return func(c *config) {
		c.userAgent = userAgent
	}
}

func WithConnectionTTL(ttl time.Duration) Option {
	return func(c *config) {
		c.connectionTTL = ttl
	}
}

func WithCredentials(credentials credentials.Credentials) Option {
	return func(c *config) {
		c.credentials = credentials
	}
}

func WithOperationTimeout(operationTimeout time.Duration) Option {
	return func(c *config) {
		c.operationTimeout = operationTimeout
	}
}

func WithOperationCancelAfter(operationCancelAfter time.Duration) Option {
	return func(c *config) {
		c.operationCancelAfter = operationCancelAfter
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.dialTimeout = timeout
	}
}

func WithBalancer(balancer balancer.Balancer) Option {
	return func(c *config) {
		c.balancer = balancer
	}
}

func WithRequestsType(requestsType string) Option {
	return func(c *config) {
		c.requestsType = requestsType
	}
}

func WithMinTLSVersion(minVersion uint16) Option {
	return func(c *config) {
		c.tlsConfig.MinVersion = minVersion
	}
}

func WithTLSSInsecureSkipVerify() Option {
	return func(c *config) {
		c.tlsConfig.InsecureSkipVerify = true
	}
}

func WithGrpcOptions(option ...grpc.DialOption) Option {
	return func(c *config) {
		c.grpcOptions = append(c.grpcOptions, option...)
	}
}

func ExcludeGRPCCodesForPessimization(codes ...codes.Code) Option {
	return func(c *config) {
		c.excludeGRPCCodesForPessimization = append(
			c.excludeGRPCCodesForPessimization,
			func() (teCodes []errors.TransportErrorCode) {
				for _, c := range codes {
					teCodes = append(teCodes, errors.FromGRPCCode(c))
				}
				return teCodes
			}()...,
		)
	}
}

func New(opts ...Option) Config {
	c := defaultConfig()
	for _, o := range opts {
		o(c)
	}
	c.grpcOptions = append(
		c.grpcOptions,
		grpcCredentials(
			c.secure,
			c.tlsConfig,
		),
	)
	if c.dnsResolver {
		c.grpcOptions = append(
			c.grpcOptions,
			grpc.WithResolvers(
				resolver.New("ydb", c.trace),
			),
		)
	}
	c.meta = meta.New(
		c.database,
		c.credentials,
		c.trace,
		c.requestsType,
		c.userAgent,
	)
	return c
}

func certPool() (certPool *x509.CertPool) {
	defer func() {
		// on darwin system panic raced on checking system security
		if e := recover(); e != nil {
			certPool = x509.NewCertPool()
		}
	}()
	var err error
	certPool, err = x509.SystemCertPool()
	if err != nil {
		certPool = x509.NewCertPool()
	}
	return
}

func defaultConfig() (c *config) {
	return &config{
		balancer: balancers.Default(),
		secure:   true,
		tlsConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    certPool(),
		},
		grpcOptions: []grpc.DialOption{
			grpc.WithContextDialer(
				func(ctx context.Context, address string) (net.Conn, error) {
					return builder.New(
						ctx,
						address,
						trace.ContextDriver(ctx).Compose(c.trace),
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
			grpc.WithBlock(),
		},
	}
}
