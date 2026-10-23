package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"slo/internal/framework"
	"slo/internal/kv"
	"slo/internal/rmw"
)

func main() {
	framework.Run(newWorkload)
}

func newWorkload(ctx context.Context, fw *framework.Framework) (framework.Workload, error) {
	var workers int
	var counters uint64
	params := kv.ParseParams(fw, "native-query-rmw", func(fs *flag.FlagSet) {
		fs.IntVar(&workers, "rmw-workers", 8, "concurrent RMW writers (at least two)")
		fs.Uint64Var(&counters, "rmw-counters", 1, "shared contended RMW counters")
	})
	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	driver, err := ydb.Open(connectCtx, fw.Config.Endpoint+fw.Config.Database,
		ydb.WithRetryBudget(params.RetryBudget),
		ydb.WithSessionPoolSizeLimit(params.PoolSize()),
	)
	if err != nil {
		params.RetryBudget.Stop()

		return nil, err
	}

	db := &storage{
		driver:       driver,
		counters:     fmt.Sprintf("`%s_counters`", params.TablePath),
		ledger:       fmt.Sprintf("`%s_ledger`", params.TablePath),
		readTimeout:  params.ReadTimeout,
		writeTimeout: params.WriteTimeout,
	}

	return rmw.New(fw, params, db, workers, counters), nil
}
