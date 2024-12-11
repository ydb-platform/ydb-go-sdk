package xsql

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/conn/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/conn/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	Option interface {
		Apply(c *Connector) error
	}
	QueryBindOption interface {
		Option
		bind.Bind
	}
	tablePathPrefixOption struct {
		bind.TablePathPrefix
	}
	tableQueryOptionsOption struct {
		tableOps  []table.Option
		queryOpts []query.Option
	}
	traceDatabaseSQLOption struct {
		t    *trace.DatabaseSQL
		opts []trace.DatabaseSQLComposeOption
	}
	traceRetryOption struct {
		t    *trace.Retry
		opts []trace.RetryComposeOption
	}
	disableServerBalancerOption struct{}
	onCloseOption               func(*Connector)
	retryBudgetOption           struct {
		budget budget.Budget
	}
	bindOption struct {
		bind.Bind
	}
	queryProcessorOption Engine
)

func (t tablePathPrefixOption) Apply(c *Connector) error {
	c.pathNormalizer = t.TablePathPrefix
	c.bindings = append(c.bindings, t.TablePathPrefix)

	return nil
}

func (processor queryProcessorOption) Apply(c *Connector) error {
	c.processor = Engine(processor)

	return nil
}

func (opt bindOption) Apply(c *Connector) error {
	c.bindings = bind.Sort(append(c.bindings, opt.Bind))

	return nil
}

func (opt retryBudgetOption) Apply(c *Connector) error {
	c.retryBudget = opt.budget

	return nil
}

func (opt traceRetryOption) Apply(c *Connector) error {
	c.traceRetry = c.traceRetry.Compose(opt.t, opt.opts...)

	return nil
}

func (onClose onCloseOption) Apply(c *Connector) error {
	c.onCLose = append(c.onCLose, onClose)

	return nil
}

func (disableServerBalancerOption) Apply(c *Connector) error {
	c.disableServerBalancer = true

	return nil
}

func (opt traceDatabaseSQLOption) Apply(c *Connector) error {
	c.trace = c.trace.Compose(opt.t, opt.opts...)

	return nil
}

func (opt tableQueryOptionsOption) Apply(c *Connector) error {
	c.QueryOpts = append(c.QueryOpts, opt.queryOpts...)
	c.TableOpts = append(c.TableOpts, opt.tableOps...)

	return nil
}

func WithTrace(
	t *trace.DatabaseSQL, //nolint:gocritic
	opts ...trace.DatabaseSQLComposeOption,
) Option {
	return traceDatabaseSQLOption{
		t:    t,
		opts: opts,
	}
}

func WithDisableServerBalancer() Option {
	return disableServerBalancerOption{}
}

func WithOnClose(onClose func(*Connector)) Option {
	return onCloseOption(onClose)
}

func WithTraceRetry(
	t *trace.Retry,
	opts ...trace.RetryComposeOption,
) Option {
	return traceRetryOption{
		t:    t,
		opts: opts,
	}
}

func WithRetryBudget(budget budget.Budget) Option {
	return retryBudgetOption{
		budget: budget,
	}
}

func WithTablePathPrefix(tablePathPrefix string) QueryBindOption {
	return tablePathPrefixOption{
		bind.TablePathPrefix(tablePathPrefix),
	}
}

func WithQueryBind(bind bind.Bind) QueryBindOption {
	return bindOption{
		Bind: bind,
	}
}

func WithDefaultQueryMode(mode table.QueryMode) Option {
	return tableQueryOptionsOption{
		tableOps: []table.Option{
			table.WithDefaultQueryMode(mode),
		},
	}
}

func WithFakeTx(modes ...table.QueryMode) Option {
	return tableQueryOptionsOption{
		tableOps: []table.Option{
			table.WithFakeTxModes(modes...),
		},
	}
}

func WithIdleThreshold(idleThreshold time.Duration) Option {
	return tableQueryOptionsOption{
		tableOps: []table.Option{
			table.WithIdleThreshold(idleThreshold),
		},
	}
}

type mergedOptions []Option

func (opts mergedOptions) Apply(c *Connector) error {
	for _, opt := range opts {
		if err := opt.Apply(c); err != nil {
			return err
		}
	}

	return nil
}

func Merge(opts ...Option) Option {
	return mergedOptions(opts)
}

func WithTableOptions(opts ...table.Option) Option {
	return tableQueryOptionsOption{
		tableOps: opts,
	}
}

func WithQueryOptions(opts ...query.Option) Option {
	return tableQueryOptionsOption{
		queryOpts: opts,
	}
}

func OverQueryService() Option {
	return queryProcessorOption(QUERY_SERVICE)
}

func OverTableService() Option {
	return queryProcessorOption(TABLE_SERVICE)
}