package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"time"

	"google.golang.org/grpc"
	grpcCredentials "google.golang.org/grpc/credentials"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancer"
	ydbCredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/resolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultDiscoveryInterval = time.Minute
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
	Credentials() ydbCredentials.Credentials

	// Trace contains driver tracing options.
	Trace() trace.Driver

	// RequestTimeout is the maximum amount of time a Call() will wait for an
	// operation to complete.
	// If RequestTimeout is zero then no timeout is used.
	RequestTimeout() time.Duration

	// StreamTimeout is the maximum amount of time a StreamRead() will wait for
	// an operation to complete.
	// If StreamTimeout is zero then no timeout is used.
	StreamTimeout() time.Duration

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

	// DiscoveryInterval is the frequency of background tasks of ydb endpoints
	// discovery.
	// If DiscoveryInterval is zero then the DefaultDiscoveryInterval is used.
	// If DiscoveryInterval is negative, then no background discovery prepared.
	DiscoveryInterval() time.Duration

	// Balancer is an optional configuration related to selected balancer.
	// That is, some balancing methods allow to be configured.
	Balancer() ibalancer.Balancer

	// RequestsType set an additional types hint to all requests.
	// It is needed only for debug purposes and advanced cases.
	RequestsType() string

	// DialTimeout is the maximum amount of time a dial will wait for a connect to
	// complete.
	// If DialTimeout is zero then no timeout is used.
	DialTimeout() time.Duration

	// TLSConfig specifies the TLS configuration to use for tls client.
	// If TLSConfig is zero then connections are insecure.
	TLSConfig() *tls.Config

	// GrpcDialOptions is an custom client grpc dial options which will appends to
	// default grpc dial options
	GrpcDialOptions() []grpc.DialOption

	// ConnectionTTL is a time to live of a connection
	// If ConnectionTTL is zero then TTL is not used.
	ConnectionTTL() time.Duration

	// Meta is an option which contains meta information about database connection
	Meta() meta.Meta
}

// Config contains driver configuration options.
type config struct {
	trace                trace.Driver
	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration
	discoveryInterval    time.Duration
	dialTimeout          time.Duration
	connectionTTL        time.Duration
	balancer             ibalancer.Balancer
	secure               bool
	endpoint             string
	database             string
	requestsType         string
	userAgent            string
	grpcOptions          []grpc.DialOption
	credentials          credentials.Credentials
	tlsConfig            *tls.Config
	meta                 meta.Meta
}

func (c *config) Meta() meta.Meta {
	return c.meta
}

func (c *config) ConnectionTTL() time.Duration {
	return c.connectionTTL
}

func (c *config) GrpcDialOptions() (opts []grpc.DialOption) {
	// nolint:gocritic
	opts = append(
		c.grpcOptions,
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return newConn(ctx, address, trace.ContextDriver(ctx).Compose(c.trace))
		}),
		grpc.WithKeepaliveParams(DefaultGrpcConnectionPolicy),
		grpc.WithResolvers(
			resolver.New(""), // for use this resolver by default
			resolver.New("grpc"),
			resolver.New("grpcs"),
		),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultGRPCMsgSize),
			grpc.MaxCallSendMsgSize(DefaultGRPCMsgSize),
		),
		grpc.WithBlock(),
	)
	if c.secure {
		opts = append(opts, grpc.WithTransportCredentials(
			grpcCredentials.NewTLS(c.tlsConfig),
		))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	return
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

func (c *config) RequestTimeout() time.Duration {
	return c.requestTimeout
}

func (c *config) StreamTimeout() time.Duration {
	return c.streamTimeout
}

func (c *config) OperationTimeout() time.Duration {
	return c.operationTimeout
}

func (c *config) OperationCancelAfter() time.Duration {
	return c.operationCancelAfter
}

func (c *config) DiscoveryInterval() time.Duration {
	return c.discoveryInterval
}

func (c *config) Balancer() ibalancer.Balancer {
	return c.balancer
}

func (c *config) RequestsType() string {
	return c.requestsType
}

type Option func(c *config)

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

func WithRequestTimeout(requestTimeout time.Duration) Option {
	return func(c *config) {
		c.requestTimeout = requestTimeout
	}
}

func WithStreamTimeout(streamTimeout time.Duration) Option {
	return func(c *config) {
		c.streamTimeout = streamTimeout
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

func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(c *config) {
		c.discoveryInterval = discoveryInterval
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.dialTimeout = timeout
	}
}

func WithBalancer(balancer ibalancer.Balancer) Option {
	return func(c *config) {
		c.balancer = balancer
	}
}

func WithRequestsType(requestsType string) Option {
	return func(c *config) {
		c.requestsType = requestsType
	}
}

func WithGrpcOptions(option ...grpc.DialOption) Option {
	return func(c *config) {
		c.grpcOptions = append(c.grpcOptions, option...)
	}
}

func New(opts ...Option) Config {
	c := defaultConfig()
	for _, o := range opts {
		o(c)
	}
	if !c.secure {
		c.tlsConfig = nil
	}
	if c.discoveryInterval == 0 {
		c.balancer = balancer.SingleConn()
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
		discoveryInterval: DefaultDiscoveryInterval,
		balancer:          balancer.Default(),
		tlsConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    certPool(),
		},
	}
}
