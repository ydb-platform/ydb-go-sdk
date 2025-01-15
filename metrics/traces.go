package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func WithTraces(config Config) ydb.Option {
	if config == nil {
		return nil
	}
	config = config.WithSystem("ydb")

	return ydb.MergeOptions(
		ydb.WithTraceDriver(driver(config)),
		ydb.WithTraceTable(table(config)),
		ydb.WithTraceQuery(query(config)),
		ydb.WithTraceScripting(scripting(config)),
		ydb.WithTraceScheme(scheme(config)),
		ydb.WithTraceCoordination(coordination(config)),
		ydb.WithTraceRatelimiter(ratelimiter(config)),
		ydb.WithTraceDiscovery(discovery(config)),
		ydb.WithTraceDatabaseSQL(DatabaseSQL(config)),
		ydb.WithTraceRetry(retry(config)),
	)
}
