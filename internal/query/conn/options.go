package conn

import (
	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(c *conn)

func WithTrace(
	t *trace.DatabaseSQL,
	opts ...trace.DatabaseSQLComposeOption,
) Option {
	return func(c *conn) {
		c.trace = c.trace.Compose(t, opts...)
	}
}

func WithTraceRetry(
	t *trace.Retry,
	opts ...trace.RetryComposeOption,
) Option {
	return func(c *conn) {
		c.traceRetry = c.traceRetry.Compose(t, opts...)
	}
}

func WithRetryBudget(budget budget.Budget) Option {
	return func(c *conn) {
		c.retryBudget = budget
	}
}

func WithQueryBindings(binds ...bind.Bind) Option {
	return func(c *conn) {
		c.bindings = append(c.bindings, binds...)
	}
}

func WithClock(clock clockwork.Clock) Option {
	return func(c *conn) {
		c.clock = clock
	}
}

func WithOnClose(onClose func()) Option {
	return func(c *conn) {
		c.onClose = append(c.onClose, onClose)
	}
}
