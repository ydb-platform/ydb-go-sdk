package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Config contains driver configuration.
type Config struct {
	config.Common

	trace              *trace.Driver
	dialTimeout        time.Duration
	connectionTTL      time.Duration
	balancerConfig     *balancerConfig.Config
	secure             bool
	endpoint           string
	database           string
	metaOptions        []meta.Option
	grpcOptions        []grpc.DialOption
	grpcMaxMessageSize int
	credentials        credentials.Credentials
	tlsConfig          *tls.Config
	meta               *meta.Meta

	excludeGRPCCodesForPessimization []grpcCodes.Code
	disableOptimisticUnban           bool

	onlyIPv6 bool
}

func (c *Config) Credentials() credentials.Credentials {
	return c.credentials
}

// ExcludeGRPCCodesForPessimization defines grpc codes for exclude its from pessimization trigger
func (c *Config) ExcludeGRPCCodesForPessimization() []grpcCodes.Code {
	return c.excludeGRPCCodesForPessimization
}

func (c *Config) DisableOptimisticUnban() bool {
	return c.disableOptimisticUnban
}

// OnlyIPv6 reports whether only IPv6 connections must be used.
// When true, a custom gRPC context dialer is injected (via WithContextDialer)
// that drops any attempt to connect to an IPv4 address and, for FQDN targets,
// resolves DNS and selects only IPv6 results.
func (c *Config) OnlyIPv6() bool {
	return c.onlyIPv6
}

// GrpcDialOptions reports about used grpc dialing options
func (c *Config) GrpcDialOptions() []grpc.DialOption {
	opts := append(
		defaultGrpcOptions(c.secure, c.tlsConfig),
		c.grpcOptions...,
	)
	if c.onlyIPv6 {
		opts = append(opts, grpc.WithContextDialer(ipv6OnlyDialer))
	}

	return opts
}

// GrpcMaxMessageSize return client settings for max grpc message size
func (c *Config) GrpcMaxMessageSize() int {
	return c.grpcMaxMessageSize
}

// Meta reports meta information about database connection
func (c *Config) Meta() *meta.Meta {
	return c.meta
}

// ConnectionTTL defines interval for parking grpc connections.
//
// If ConnectionTTL is zero - connections are not park.
func (c *Config) ConnectionTTL() time.Duration {
	return c.connectionTTL
}

// Secure is a flag for secure connection
func (c *Config) Secure() bool {
	return c.secure
}

// Endpoint is a required starting endpoint for connect
func (c *Config) Endpoint() string {
	return c.endpoint
}

// TLSConfig reports about TLS configuration
func (c *Config) TLSConfig() *tls.Config {
	return c.tlsConfig
}

// DialTimeout is the maximum amount of time a dial will wait for a connect to
// complete.
//
// If DialTimeout is zero then no timeout is used.
func (c *Config) DialTimeout() time.Duration {
	return c.dialTimeout
}

// Database is a required database name.
func (c *Config) Database() string {
	return c.database
}

// Trace contains driver tracing options.
func (c *Config) Trace() *trace.Driver {
	return c.trace
}

// Balancer is an optional configuration related to selected balancer.
// That is, some balancing methods allow to be configured.
func (c *Config) Balancer() *balancerConfig.Config {
	return c.balancerConfig
}

type Option func(c *Config)

// WithInternalDNSResolver
//
// Deprecated: always used internal dns-resolver.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
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

func WithTrace(t trace.Driver, opts ...trace.DriverComposeOption) Option { //nolint:gocritic
	return func(c *Config) {
		c.trace = c.trace.Compose(&t, opts...)
	}
}

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithRetryBudget(b budget.Budget) Option {
	return func(c *Config) {
		config.SetRetryBudget(&c.Common, b)
	}
}

func WithTraceRetry(t *trace.Retry, opts ...trace.RetryComposeOption) Option {
	return func(c *Config) {
		config.SetTraceRetry(&c.Common, t, opts...)
	}
}

// WithApplicationName add provided application name to all api requests
func WithApplicationName(applicationName string) Option {
	return func(c *Config) {
		c.metaOptions = append(c.metaOptions, meta.WithApplicationNameOption(applicationName))
	}
}

// WithBuildInfo adds provided framework name with its version to all API requests
// via the x-ydb-sdk-build-info header. The frameworkName and frameworkVersion
// must not contain semicolons (";"). Invalid values are silently ignored.
func WithBuildInfo(frameworkName string, frameworkVersion string) Option {
	if err := meta.ValidateBuildInfo(frameworkName, frameworkVersion); err != nil {
		return nil // wrong frameworkName or frameworkVersion will not apply
	}

	return func(c *Config) {
		c.metaOptions = append(c.metaOptions, meta.WithBuildInfo(frameworkName, frameworkVersion))
	}
}

// WithUserAgent add provided user agent to all api requests
//
// Deprecated: use WithApplicationName instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithUserAgent(userAgent string) Option {
	return func(c *Config) {
		c.metaOptions = append(c.metaOptions, meta.WithApplicationNameOption(userAgent))
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
func WithPanicCallback(panicCallback func(e any)) Option {
	return func(c *Config) {
		config.SetPanicCallback(&c.Common, panicCallback)
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.dialTimeout = timeout
	}
}

func WithGrpcMaxMessageSize(sizeBytes int) Option {
	return func(c *Config) {
		c.grpcMaxMessageSize = sizeBytes
		c.grpcOptions = append(c.grpcOptions,
			// limit size of outgoing and incoming packages
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(c.grpcMaxMessageSize),
				grpc.MaxCallSendMsgSize(c.grpcMaxMessageSize),
			),
		)
	}
}

func WithBalancer(balancer *balancerConfig.Config) Option {
	return func(c *Config) {
		c.balancerConfig = balancer
	}
}

func WithRequestsType(requestsType string) Option {
	return func(c *Config) {
		c.metaOptions = append(c.metaOptions, meta.WithRequestTypeOption(requestsType))
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

// WithGrpcOptions appends custom grpc dial options to defaults
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

// WithDisableOptimisticUnban disables optimistic unban of nodes after a successful call
// immediately following pessimization.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithDisableOptimisticUnban() Option {
	return func(c *Config) {
		c.disableOptimisticUnban = true
	}
}

// WithOnlyIPv6 sets a flag that instructs gRPC to connect over IPv6 only.
// A custom ContextDialer is injected into the gRPC dial options so that:
//   - direct IPv4 address targets are rejected;
//   - for FQDN targets, DNS is resolved and only IPv6 results are used.
//
// This option is used internally by ydb.WithOnlyIPv6.
func WithOnlyIPv6() Option {
	return func(c *Config) {
		c.onlyIPv6 = true
	}
}

func New(opts ...Option) *Config {
	c := defaultConfig()

	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	c.meta = meta.New(c.database, c.credentials, c.trace, c.metaOptions...)

	return c
}

// With makes copy of current Config with specified options
func (c *Config) With(opts ...Option) *Config {
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	c.meta = meta.New(
		c.database,
		c.credentials,
		c.trace,
		c.metaOptions...,
	)

	return c
}

// ipv6OnlyDialer is a gRPC ContextDialer that refuses IPv4 connections.
//
// When gRPC dials an endpoint the target address passed to this function may be:
//   - an IPv4 literal  ("1.2.3.4:2135")    → rejected with an error;
//   - an IPv6 literal  ("[::1]:2135")       → dialled directly over tcp6;
//   - an FQDN          ("host.example:2135") → DNS is resolved and only the
//     first IPv6 result is used; if there are none an error is returned.
func ipv6OnlyDialer(ctx context.Context, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// addr has no port – treat the whole string as the host.
		host = addr
		port = ""
	}

	if ip := net.ParseIP(host); ip != nil {
		// The address is already a resolved IP literal.
		if ip.To4() != nil {
			return nil, fmt.Errorf("ydb: OnlyIPv6 option is set but got IPv4 address %q", addr)
		}
		// IPv6 literal – dial directly.
		return (&net.Dialer{}).DialContext(ctx, "tcp6", addr)
	}

	// FQDN: resolve via DNS and pick the first IPv6 result.
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("ydb: DNS lookup for %q failed: %w", host, err)
	}

	for _, resolved := range ips {
		ip := net.ParseIP(resolved)
		if ip == nil || ip.To4() != nil {
			// skip non-parseable or IPv4 results
			continue
		}
		target := net.JoinHostPort(resolved, port)

		return (&net.Dialer{}).DialContext(ctx, "tcp6", target)
	}

	return nil, fmt.Errorf("ydb: OnlyIPv6 option is set but no IPv6 address found for %q", host)
}
