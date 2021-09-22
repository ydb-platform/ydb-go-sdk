package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"time"
)

// Config contains driver configuration options.
type Config struct {
	// Database is a required database name.
	Database string

	// Credentials is an ydb client credentials.
	// In most cases Credentials are required.
	Credentials credentials.Credentials

	// Trace contains driver tracing options.
	Trace trace.DriverTrace

	// RequestTimeout is the maximum amount of time a Call() will wait for an
	// operation to complete.
	// If RequestTimeout is zero then no timeout is used.
	RequestTimeout time.Duration

	// StreamTimeout is the maximum amount of time a StreamRead() will wait for
	// an operation to complete.
	// If StreamTimeout is zero then no timeout is used.
	StreamTimeout time.Duration

	// OperationTimeout is the maximum amount of time a YDB server will process
	// an operation. After timeout exceeds YDB will try to cancel operation and
	// regardless of the cancellation appropriate error will be returned to
	// the client.
	// If OperationTimeout is zero then no timeout is used.
	OperationTimeout time.Duration

	// OperationCancelAfter is the maximum amount of time a YDB server will process an
	// operation. After timeout exceeds YDB will try to cancel operation and if
	// it succeeds appropriate error will be returned to the client; otherwise
	// processing will be continued.
	// If OperationCancelAfter is zero then no timeout is used.
	OperationCancelAfter time.Duration

	// DiscoveryInterval is the frequency of background tasks of ydb endpoints
	// discovery.
	// If DiscoveryInterval is zero then the DefaultDiscoveryInterval is used.
	// If DiscoveryInterval is negative, then no background discovery prepared.
	DiscoveryInterval time.Duration

	// GrpcConnectionPolicy define lifecycle behaviour of grpc connection
	// By default GrpcConnectionPolicy is sets to DefaultGrpcConnectionPolicy
	GrpcConnectionPolicy *GrpcConnectionPolicy

	// BalancingConfig is an optional configuration related to selected
	// BalancingMethod. That is, some balancing methods allow to be configured.
	BalancingConfig balancer.Config

	// RequestsType set an additional types hint to all requests.
	// It is needed only for debug purposes and advanced cases.
	RequestsType string

	// FastDial will make dialer return Driver as soon as 1st connection succeeds.
	// NB: it may be not the fastest node to serve requests.
	FastDial bool
}

type option func(c *Config)

func New(opts ...option) *Config {
	c := defaults()
	for _, o := range opts {
		o(c)
	}
	return c
}

func defaults() (c *Config) {
	return &Config{
		DiscoveryInterval:    discovery.DefaultDiscoveryInterval,
		GrpcConnectionPolicy: &DefaultGrpcConnectionPolicy,
		BalancingConfig:      balancer.Default,
	}
}
