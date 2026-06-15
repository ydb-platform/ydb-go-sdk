package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Config is a configuration of ratelimiter client
type Config struct {
	config.Common

	trace *trace.Ratelimiter
}

// Trace returns trace over ratelimiter calls
func (c Config) Trace() *trace.Ratelimiter {
	return c.trace
}

type Option func(c *Config)

// WithTrace appends ratelimiter trace to early defined traces
func WithTrace(t trace.Ratelimiter) Option {
	return func(c *Config) {
		var opts []gtrace.RatelimiterComposeOption
		if cb := c.PanicCallback(); cb != nil {
			opts = append(opts, gtrace.WithRatelimiterPanicCallback(cb))
		}
		c.trace = gtrace.Compose(c.trace, &t, opts...)
	}
}

// With applies common configuration params
func With(config config.Common) Option {
	return func(c *Config) {
		c.Common = config
	}
}

func New(opts ...Option) Config {
	c := Config{
		trace: &trace.Ratelimiter{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}

	return c
}
