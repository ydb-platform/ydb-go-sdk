package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultInterval = time.Minute
)

type Config interface {
	Endpoint() string
	Database() string
	Secure() bool
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
	interval time.Duration
	trace    trace.Discovery
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
		c.trace = trace
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
