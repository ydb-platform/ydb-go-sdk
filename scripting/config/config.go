package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
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

	// Trace defines trace over scripting client calls
	Trace() trace.Scripting
}

type config struct {
	operationTimeout     time.Duration
	operationCancelAfter time.Duration

	trace trace.Scripting
}

func (c *config) Trace() trace.Scripting {
	return c.trace
}

func (c *config) OperationTimeout() time.Duration {
	return c.operationTimeout
}

func (c *config) OperationCancelAfter() time.Duration {
	return c.operationCancelAfter
}

type Option func(c *config)

func WithTrace(trace trace.Scripting, opts ...trace.ScriptingComposeOption) Option {
	return func(c *config) {
		c.trace = c.trace.Compose(trace, opts...)
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

func New(opts ...Option) Config {
	c := &config{}
	for _, o := range opts {
		o(c)
	}
	return c
}
