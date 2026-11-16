package ydb

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	tableTrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type driverSessionPoolConfig struct {
	limit          int
	warmUp         int
	itemUsageLimit uint64
	itemUsageTTL   time.Duration
	idleTTL        time.Duration
	createTimeout  time.Duration
	deleteTimeout  time.Duration
}

func defaultDriverSessionPoolConfig() driverSessionPoolConfig {
	return driverSessionPoolConfig{
		limit:         pool.DefaultLimit,
		idleTTL:       tableConfig.DefaultSessionPoolIdleThreshold,
		createTimeout: queryConfig.DefaultSessionCreateTimeout,
		deleteTimeout: queryConfig.DefaultSessionDeleteTimeout,
	}
}

func (d *Driver) sharedSessionPoolSettings(queryCfg *queryConfig.Config) *queryConfig.Config {
	cfg := d.sessionPoolConfig
	tableCfg := tableConfig.New(append([]tableConfig.Option{tableConfig.With(d.config.Common)}, d.tableOptions...)...)

	return queryConfig.New(
		queryConfig.With(queryCfg.Common),
		queryConfig.WithPoolLimit(cfg.limit),
		queryConfig.WithSessionPoolWarmUpSessions(cfg.warmUp),
		queryConfig.WithSessionPoolSessionUsageLimit(cfg.itemUsageLimit),
		queryConfig.WithSessionPoolSessionUsageLimit(cfg.itemUsageTTL),
		queryConfig.WithSessionIdleTimeToLive(cfg.idleTTL),
		queryConfig.WithSessionCreateTimeout(cfg.createTimeout),
		queryConfig.WithSessionDeleteTimeout(cfg.deleteTimeout),
		queryConfig.WithPoolName(queryCfg.PoolName()),
		queryConfig.WithTrace(queryCfg.Trace()),
		queryConfig.WithTrace(sharedTablePoolTrace(tableCfg.Trace())),
	)
}

func sharedTablePoolTrace(t *trace.Table) *trace.Query {
	return &trace.Query{
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(trace.QuerySessionCreateDoneInfo) {
			done := tableTrace.TableOnSessionNew(t, info.Context, info.Call)

			return func(info trace.QuerySessionCreateDoneInfo) { done(info.Session, info.Error) }
		},
		OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(trace.QuerySessionDeleteDoneInfo) {
			done := tableTrace.TableOnSessionDelete(t, info.Context, info.Call, info.Session)

			return func(info trace.QuerySessionDeleteDoneInfo) { done(info.Error) }
		},
		OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
			done := tableTrace.TableOnPoolGet(t, info.Context, info.Call)

			return func(info trace.QueryPoolGetDoneInfo) {
				done(info.Session, info.Attempts, info.NodeHintInfo, info.Error)
			}
		},
		OnPoolPut: func(info trace.QueryPoolPutStartInfo) func(trace.QueryPoolPutDoneInfo) {
			done := tableTrace.TableOnPoolPut(t, info.Context, info.Call, info.Session)

			return func(info trace.QueryPoolPutDoneInfo) { done(info.Error) }
		},
		OnPoolWith: func(info trace.QueryPoolWithStartInfo) func(trace.QueryPoolWithDoneInfo) {
			done := tableTrace.TableOnPoolWith(t, info.Context, info.Call)

			return func(info trace.QueryPoolWithDoneInfo) { done(info.Attempts, info.Error) }
		},
		OnPoolChange: func(info trace.QueryPoolChange) {
			tableTrace.TableOnPoolStateChange(t, info.Limit, info.Idle, info.CreateInProgress, info.Concurrency, info.Size)
		},
	}
}
