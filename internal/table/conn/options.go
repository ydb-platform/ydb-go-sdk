package conn

import (
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(*conn)

func WithFakeTxModes(modes ...QueryMode) Option {
	return func(c *conn) {
		for _, m := range modes {
			c.beginTxFuncs[m] = beginTxFake
		}
	}
}

func WithDataOpts(dataOpts ...options.ExecuteDataQueryOption) Option {
	return func(c *conn) {
		c.dataOpts = dataOpts
	}
}

func WithScanOpts(scanOpts ...options.ExecuteScanQueryOption) Option {
	return func(c *conn) {
		c.scanOpts = scanOpts
	}
}

func WithDefaultTxControl(defaultTxControl *table.TransactionControl) Option {
	return func(c *conn) {
		c.defaultTxControl = defaultTxControl
	}
}

func WithDefaultQueryMode(mode QueryMode) Option {
	return func(c *conn) {
		c.defaultQueryMode = mode
	}
}

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

func WithIdleThreshold(idleThreshold time.Duration) Option {
	return func(c *conn) {
		c.idleThreshold = idleThreshold
	}
}

func WithOnClose(onCLose func()) Option {
	return func(c *conn) {
		c.onClose = append(c.onClose, onCLose)
	}
}

func WithClock(clock clockwork.Clock) Option {
	return func(c *conn) {
		c.clock = clock
	}
}
