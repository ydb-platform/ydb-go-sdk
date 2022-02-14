package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
	Trace() trace.Coordination
}

type config struct {
	trace trace.Coordination
}

func (c *config) Trace() trace.Coordination {
	return c.trace
}

type Option func(c *config)

func WithTrace(trace trace.Coordination) Option {
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
