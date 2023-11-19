package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultInterval = time.Minute
)

type Config struct {
	config.Common

	endpoint string
	database string
	secure   bool
	meta     *meta.Meta

	interval time.Duration
	trace    *trace.Discovery
}

func New(opts ...Option) *Config {
	c := &Config{
		interval: DefaultInterval,
		trace:    &trace.Discovery{},
	}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	return c
}

func (c *Config) Meta() *meta.Meta {
	return c.meta
}

func (c *Config) Interval() time.Duration {
	return c.interval
}

func (c *Config) Endpoint() string {
	return c.endpoint
}

func (c *Config) Database() string {
	return c.database
}

func (c *Config) Secure() bool {
	return c.secure
}

func (c *Config) Trace() *trace.Discovery {
	return c.trace
}

type Option func(c *Config)

// With applies common configuration params
func With(config config.Common) Option {
	return func(c *Config) {
		c.Common = config
	}
}

// WithEndpoint set a required starting endpoint for connect
func WithEndpoint(endpoint string) Option {
	return func(c *Config) {
		c.endpoint = endpoint
	}
}

// WithDatabase set a required database name.
func WithDatabase(database string) Option {
	return func(c *Config) {
		c.database = database
	}
}

// WithSecure set flag for secure connection
func WithSecure(ssl bool) Option {
	return func(c *Config) {
		c.secure = ssl
	}
}

// WithMeta is not for user.
//
// This option add meta information about database connection
func WithMeta(meta *meta.Meta) Option {
	return func(c *Config) {
		c.meta = meta
	}
}

// WithTrace configures discovery client calls tracing
func WithTrace(trace trace.Discovery, opts ...trace.DiscoveryComposeOption) Option {
	return func(c *Config) {
		c.trace = c.trace.Compose(&trace, opts...)
	}
}

// WithInterval set the frequency of background tasks of ydb endpoints discovery.
//
// If Interval is zero then the DefaultInterval is used.
//
// If Interval is negative, then no background discovery prepared.
func WithInterval(interval time.Duration) Option {
	return func(c *Config) {
		switch {
		case interval < 0:
			c.interval = 0
		case interval == 0:
			c.interval = DefaultInterval
		default:
			c.interval = interval
		}
	}
}
