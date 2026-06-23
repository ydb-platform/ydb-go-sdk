package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config struct {
	config.Common

	trace *trace.Scripting
}

// Trace defines trace over scripting client calls
func (c Config) Trace() *trace.Scripting {
	return c.trace
}

type Option func(c *Config)

// WithTrace appends scripting trace to early added traces
func WithTrace(t trace.Scripting) Option {
	return func(c *Config) {
		var opts []gtrace.ScriptingComposeOption
		if cb := c.PanicCallback(); cb != nil {
			opts = append(opts, gtrace.WithScriptingPanicCallback(cb))
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
		trace: &trace.Scripting{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}

	return c
}
