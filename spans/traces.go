package spans

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func WithTraces(adapter Adapter) ydb.Option {
	if adapter == nil {
		return nil
	}

	return ydb.MergeOptions(
		ydb.WithTraceDriver(driver(adapter)),
		ydb.WithTraceTable(table(adapter)),
		ydb.WithTraceQuery(query(adapter)),
		ydb.WithTraceScripting(scripting(adapter)),
		ydb.WithTraceScheme(scheme(adapter)),
		ydb.WithTraceCoordination(coordination(adapter)),
		ydb.WithTraceRatelimiter(ratelimiter(adapter)),
		ydb.WithTraceDiscovery(discovery(adapter)),
		ydb.WithTraceDatabaseSQL(databaseSQL(adapter)),
		ydb.WithTraceRetry(Retry(adapter)),
	)
}
