package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultInterval = time.Minute
)

type Config interface {
	// Endpoint is a required starting endpoint for connect
	Endpoint() string

	// Database is a required database name.
	Database() string

	// Secure is an flag for secure connection
	Secure() bool

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

	// Trace defines trace over discovery client calls
	Trace() trace.Discovery

	// Interval is the frequency of background tasks of ydb endpoints discovery.
	// If Interval is zero then the DefaultInterval is used.
	// If Interval is negative, then no background discovery prepared.
	Interval() time.Duration
}

type config struct {
	endpoint string
	database string
	secure   bool

	operationTimeout     time.Duration
	operationCancelAfter time.Duration

	interval time.Duration
	trace    trace.Discovery
}

func (c *config) OperationTimeout() time.Duration {
	return c.operationTimeout
}

func (c *config) OperationCancelAfter() time.Duration {
	return c.operationCancelAfter
}

func (c *config) Interval() time.Duration {
	return c.interval
}

func (c *config) Endpoint() string {
	return c.endpoint
}

func (c *config) Database() string {
	return c.database
}

func (c *config) Secure() bool {
	return c.secure
}

func (c *config) Trace() trace.Discovery {
	return c.trace
}

type Option func(c *config)

func WithEndpoint(endpoint string) Option {
	return func(c *config) {
		c.endpoint = endpoint
	}
}

func WithDatabase(database string) Option {
	return func(c *config) {
		c.database = database
	}
}

func WithSecure(ssl bool) Option {
	return func(c *config) {
		c.secure = ssl
	}
}

func WithTrace(trace trace.Discovery) Option {
	return func(c *config) {
		c.trace = c.trace.Compose(trace)
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

func WithInterval(interval time.Duration) Option {
	return func(c *config) {
		if interval <= 0 {
			c.interval = 0
		} else {
			c.interval = interval
		}
	}
}

func New(opts ...Option) Config {
	c := &config{
		interval: DefaultInterval,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}
