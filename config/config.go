package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
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

	// Secure() is an flag for secure connection
	Secure() bool

	// Credentials is an ydb client credentials.
	// In most cases Credentials are required.
	Credentials() credentials.Credentials

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

	// GrpcConnectionPolicy define lifecycle behavior of grpc connection
	// By default GrpcConnectionPolicy is sets to DefaultGrpcConnectionPolicy
	GrpcConnectionPolicy() GrpcConnectionPolicy

	// BalancingConfig is an optional configuration related to selected
	// BalancingMethod. That is, some balancing methods allow to be configured.
	BalancingConfig() BalancerConfig

	// RequestsType set an additional types hint to all requests.
	// It is needed only for debug purposes and advanced cases.
	RequestsType() string

	// FastDial will make dialer return Driver as soon as 1st connection succeeds.
	// NB: it may be not the fastest node to serve requests.
	FastDial() bool

	// DialTimeout is the maximum amount of time a dial will wait for a connect to
	// complete.
	// If DialTimeout is zero then no timeout is used.
	DialTimeout() time.Duration

	// TLSConfig specifies the TLS configuration to use for tls client.
	// If TLSConfig is zero then connections are insecure.
	TLSConfig() *tls.Config

	// NetDial is an optional function that may replace default network dialing
	// function such as net.Dial("tcp").
	NetDial() func(context.Context, string) (net.Conn, error)
}

// Config contains driver configuration options.
type config struct {
	endpoint             string
	database             string
	secure               bool
	credentials          credentials.Credentials
	trace                trace.Driver
	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration
	discoveryInterval    time.Duration
	grpcConnectionPolicy GrpcConnectionPolicy
	balancingConfig      BalancerConfig
	requestsType         string
	fastDial             bool
	dialTimeout          time.Duration
	tlsConfig            *tls.Config
	netDial              func(context.Context, string) (net.Conn, error)
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

func (c *config) NetDial() func(context.Context, string) (net.Conn, error) {
	return c.netDial
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

func (c *config) GrpcConnectionPolicy() GrpcConnectionPolicy {
	return c.grpcConnectionPolicy
}

func (c *config) BalancingConfig() BalancerConfig {
	return c.balancingConfig
}

func (c *config) RequestsType() string {
	return c.requestsType
}

func (c *config) FastDial() bool {
	return c.fastDial
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

func WithGrpcConnectionTTL(ttl time.Duration) Option {
	return func(c *config) {
		c.grpcConnectionPolicy.TTL = ttl
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.dialTimeout = timeout
	}
}

func WithGrpcConnectionPolicy(grpcConnectionPolicy GrpcConnectionPolicy) Option {
	return func(c *config) {
		c.grpcConnectionPolicy = grpcConnectionPolicy
	}
}

func WithBalancingConfig(balancingConfig BalancerConfig) Option {
	return func(c *config) {
		c.balancingConfig = balancingConfig
	}
}

func WithRequestsType(requestsType string) Option {
	return func(c *config) {
		c.requestsType = requestsType
	}
}

func WithFastDial(fastDial bool) Option {
	return func(c *config) {
		c.fastDial = fastDial
	}
}

func WithNetDial(netDial func(context.Context, string) (net.Conn, error)) Option {
	return func(c *config) {
		c.netDial = netDial
	}
}

func New(opts ...Option) Config {
	c := defaults()
	for _, o := range opts {
		o(c)
	}
	if !c.secure {
		c.tlsConfig = nil
	}
	return c
}

func defaults() (c *config) {
	var certPool *x509.CertPool
	certPool, err := x509.SystemCertPool()
	if err != nil {
		panic(fmt.Errorf("cannot load system certificates pool: %v", err))
	}
	if caFile, has := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); has {
		// ignore any errors on load certificates
		if err = credentials.AppendCertsFromFile(certPool, caFile); err != nil {
			log.Printf("cannot load certificates from file '%s' by Env['YDB_SSL_ROOT_CERTIFICATES_FILE']: %v", caFile, err)
		}
	}
	return &config{
		discoveryInterval:    DefaultDiscoveryInterval,
		grpcConnectionPolicy: DefaultGrpcConnectionPolicy,
		balancingConfig:      DefaultBalancer,
		tlsConfig: &tls.Config{
			RootCAs: certPool,
		},
	}
}
