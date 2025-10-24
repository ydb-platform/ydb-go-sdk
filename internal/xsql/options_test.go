package xsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/xquery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/xtable"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTablePathPrefixOption_Apply(t *testing.T) {
	opt := tablePathPrefixOption{
		TablePathPrefix: bind.TablePathPrefix("/test/path"),
	}
	connector := &Connector{}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.NotNil(t, connector.pathNormalizer)
	require.Len(t, connector.bindings, 1)
}

func TestQueryProcessorOption_Apply(t *testing.T) {
	t.Run("QUERY", func(t *testing.T) {
		opt := queryProcessorOption(QUERY)
		connector := &Connector{}

		err := opt.Apply(connector)
		require.NoError(t, err)
		require.Equal(t, Engine(QUERY), connector.processor)
	})

	t.Run("TABLE", func(t *testing.T) {
		opt := queryProcessorOption(TABLE)
		connector := &Connector{}

		err := opt.Apply(connector)
		require.NoError(t, err)
		require.Equal(t, Engine(TABLE), connector.processor)
	})
}

func TestBindOption_Apply(t *testing.T) {
	opt := BindOption{
		Bind: bind.TablePathPrefix("/test"),
	}
	connector := &Connector{}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.bindings, 1)
}

func TestRetryBudgetOption_Apply(t *testing.T) {
	b := budget.Limited(100)
	opt := retryBudgetOption{
		budget: b,
	}
	connector := &Connector{}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Equal(t, b, connector.retryBudget)
}

func TestTraceRetryOption_Apply(t *testing.T) {
	tr := &trace.Retry{}
	opt := traceRetryOption{
		t:    tr,
		opts: nil,
	}
	connector := &Connector{
		traceRetry: &trace.Retry{},
	}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.NotNil(t, connector.traceRetry)
}

func TestOnCloseOption_Apply(t *testing.T) {
	called := false
	onClose := func(c *Connector) {
		called = true
	}
	opt := onCloseOption(onClose)
	connector := &Connector{}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.onClose, 1)

	connector.onClose[0](connector)
	require.True(t, called)
}

func TestDisableServerBalancerOption_Apply(t *testing.T) {
	opt := disableServerBalancerOption{}
	connector := &Connector{}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.True(t, connector.disableServerBalancer)
}

func TestTraceDatabaseSQLOption_Apply(t *testing.T) {
	tr := &trace.DatabaseSQL{}
	opt := traceDatabaseSQLOption{
		t:    tr,
		opts: nil,
	}
	connector := &Connector{
		trace: &trace.DatabaseSQL{},
	}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.NotNil(t, connector.trace)
}

func TestProcessorOptionsOption_Apply(t *testing.T) {
	tableOpt := xtable.WithDefaultQueryMode(xtable.DataQueryMode)
	queryOpt := xquery.WithFakeTx()

	opt := processorOptionsOption{
		tableOpts: []xtable.Option{tableOpt},
		queryOpts: []xquery.Option{queryOpt},
	}
	connector := &Connector{}

	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.TableOpts, 1)
	require.Len(t, connector.QueryOpts, 1)
}

func TestWithTrace(t *testing.T) {
	tr := &trace.DatabaseSQL{}
	opt := WithTrace(tr)
	require.NotNil(t, opt)

	connector := &Connector{
		trace: &trace.DatabaseSQL{},
	}
	err := opt.Apply(connector)
	require.NoError(t, err)
}

func TestWithDisableServerBalancer(t *testing.T) {
	opt := WithDisableServerBalancer()
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.True(t, connector.disableServerBalancer)
}

func TestWithOnClose(t *testing.T) {
	called := false
	onClose := func(c *Connector) {
		called = true
	}
	opt := WithOnClose(onClose)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.onClose, 1)

	connector.onClose[0](connector)
	require.True(t, called)
}

func TestWithTraceRetry(t *testing.T) {
	tr := &trace.Retry{}
	opt := WithTraceRetry(tr)
	require.NotNil(t, opt)

	connector := &Connector{
		traceRetry: &trace.Retry{},
	}
	err := opt.Apply(connector)
	require.NoError(t, err)
}

func TestWithRetryBudget(t *testing.T) {
	b := budget.Limited(100)
	opt := WithRetryBudget(b)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Equal(t, b, connector.retryBudget)
}

func TestWithTablePathPrefix(t *testing.T) {
	opt := WithTablePathPrefix("/test/path")
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.NotNil(t, connector.pathNormalizer)
}

func TestWithQueryBind(t *testing.T) {
	b := bind.TablePathPrefix("/test")
	opt := WithQueryBind(b)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.bindings, 1)
}

func TestWithDefaultQueryMode(t *testing.T) {
	opt := WithDefaultQueryMode(xtable.DataQueryMode)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.TableOpts, 1)
}

func TestWithFakeTx(t *testing.T) {
	opt := WithFakeTx(xtable.DataQueryMode, xtable.ScanQueryMode)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.TableOpts, 1)
}

func TestWithIdleThreshold(t *testing.T) {
	opt := WithIdleThreshold(time.Minute)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.TableOpts, 1)
}

func TestMergedOptions_Apply(t *testing.T) {
	opt1 := WithDisableServerBalancer()
	opt2 := WithRetryBudget(budget.Limited(100))

	merged := mergedOptions([]Option{opt1, opt2})
	connector := &Connector{}

	err := merged.Apply(connector)
	require.NoError(t, err)
	require.True(t, connector.disableServerBalancer)
	require.NotNil(t, connector.retryBudget)
}

func TestMerge(t *testing.T) {
	opt1 := WithDisableServerBalancer()
	opt2 := WithRetryBudget(budget.Limited(100))

	merged := Merge(opt1, opt2)
	require.NotNil(t, merged)

	connector := &Connector{}
	err := merged.Apply(connector)
	require.NoError(t, err)
	require.True(t, connector.disableServerBalancer)
	require.NotNil(t, connector.retryBudget)
}

func TestWithTableOptions(t *testing.T) {
	tableOpt := xtable.WithDefaultQueryMode(xtable.DataQueryMode)
	opt := WithTableOptions(tableOpt)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.TableOpts, 1)
}

func TestWithQueryOptions(t *testing.T) {
	queryOpt := xquery.WithFakeTx()
	opt := WithQueryOptions(queryOpt)
	require.NotNil(t, opt)

	connector := &Connector{}
	err := opt.Apply(connector)
	require.NoError(t, err)
	require.Len(t, connector.QueryOpts, 1)
}

func TestWithQueryService(t *testing.T) {
	t.Run("EnableQueryService", func(t *testing.T) {
		opt := WithQueryService(true)
		require.NotNil(t, opt)

		connector := &Connector{}
		err := opt.Apply(connector)
		require.NoError(t, err)
		require.Equal(t, Engine(QUERY), connector.processor)
	})

	t.Run("DisableQueryService", func(t *testing.T) {
		opt := WithQueryService(false)
		require.NotNil(t, opt)

		connector := &Connector{}
		err := opt.Apply(connector)
		require.NoError(t, err)
		require.Equal(t, Engine(TABLE), connector.processor)
	})
}
