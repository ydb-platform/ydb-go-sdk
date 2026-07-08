package ydb

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type driverSessionPoolConfig struct {
	limit           int
	warmUp          int
	itemUsageLimit  uint64
	itemUsageTTL    time.Duration
	idleTTL         time.Duration
	createTimeout   time.Duration
	deleteTimeout   time.Duration
	disableBalancer bool
}

func defaultDriverSessionPoolConfig() driverSessionPoolConfig {
	return driverSessionPoolConfig{
		limit: pool.DefaultLimit,
	}
}

func (d *Driver) sharedSessionPoolSettings(queryTrace *trace.Query) internalQuery.SharedSessionPoolConfig {
	cfg := d.sessionPoolConfig

	limit := cfg.limit
	if limit <= 0 {
		limit = pool.DefaultLimit
	}

	createTimeout := cfg.createTimeout
	if createTimeout <= 0 {
		createTimeout = queryConfig.DefaultSessionCreateTimeout
	}

	deleteTimeout := cfg.deleteTimeout
	if deleteTimeout <= 0 {
		deleteTimeout = queryConfig.DefaultSessionDeleteTimeout
	}

	idleTTL := cfg.idleTTL
	if idleTTL <= 0 {
		idleTTL = tableConfig.DefaultSessionPoolIdleThreshold
	}

	return internalQuery.SharedSessionPoolConfig{
		Limit:                  limit,
		WarmUp:                 cfg.warmUp,
		ItemUsageLimit:         cfg.itemUsageLimit,
		ItemUsageTTL:           cfg.itemUsageTTL,
		IdleTTL:                idleTTL,
		SessionCreateTimeout:   createTimeout,
		SessionDeleteTimeout:   deleteTimeout,
		DisableSessionBalancer: cfg.disableBalancer,
		Trace:                  queryTrace,
	}
}
