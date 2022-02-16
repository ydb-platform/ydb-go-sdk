package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
	Trace() trace.Scripting
}

type config struct {
	trace trace.Scripting
}

func (c *config) Trace() trace.Scripting {
	return c.trace
}

type Option func(c *config)

func WithTrace(trace trace.Scripting) Option {
	return func(c *config) {
		c.trace = trace
	}
}

func New(opts ...Option) Config {
	c := &config{}
	for _, o := range opts {
		o(c)
	}
	return c
}
