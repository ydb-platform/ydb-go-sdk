package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
	Trace() trace.Ratelimiter
	OperationParams() operation.Params
}

type config struct {
	trace           trace.Ratelimiter
	operationParams operation.Params
}

func (c *config) OperationParams() operation.Params {
	return c.operationParams
}

func (c *config) Trace() trace.Ratelimiter {
	return c.trace
}

func WithOperationTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.operationParams.Timeout = timeout
	}
}

func WithOperationCancelAfter(cancelAfter time.Duration) Option {
	return func(c *config) {
		c.operationParams.CancelAfter = cancelAfter
	}
}

type Option func(c *config)

func WithTrace(trace trace.Ratelimiter) Option {
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
