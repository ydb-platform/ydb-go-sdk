package metrics

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:funlen
func query(config Config) (t trace.Query) {
	queryConfig := config.WithSystem("query")
	{
		poolConfig := queryConfig.WithSystem("pool")
		{
			withConfig := poolConfig.WithSystem("with")
			errs := withConfig.CounterVec("errs", "status")
			latency := withConfig.TimerVec("latency")
			attempts := withConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
			t.OnPoolWith = func(
				info trace.QueryPoolWithStartInfo,
			) func(
				info trace.QueryPoolWithDoneInfo,
			) {
				if withConfig.Details()&trace.QueryPoolEvents == 0 {
					return nil
				}

				start := time.Now()

				return func(info trace.QueryPoolWithDoneInfo) {
					attempts.With(nil).Record(float64(info.Attempts))
					if info.Error != nil {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
					}
					latency.With(nil).Record(time.Since(start))
				}
			}
		}
		{
			sizeConfig := poolConfig.WithSystem("size")
			limit := sizeConfig.GaugeVec("limit")
			idle := sizeConfig.GaugeVec("idle")
			index := sizeConfig.GaugeVec("index")
			inUse := sizeConfig.WithSystem("in").GaugeVec("use")

			t.OnPoolChange = func(stats trace.QueryPoolChange) {
				if sizeConfig.Details()&trace.QueryPoolEvents == 0 {
					return
				}

				limit.With(nil).Set(float64(stats.Limit))
				idle.With(nil).Set(float64(stats.Idle))
				inUse.With(nil).Set(float64(stats.InUse))
				index.With(nil).Set(float64(stats.Index))
			}
		}
	}
	{
		doConfig := queryConfig.WithSystem("do")
		{
			errs := doConfig.CounterVec("errs", "status")
			attempts := doConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
			latency := doConfig.TimerVec("latency")
			t.OnDo = func(
				info trace.QueryDoStartInfo,
			) func(
				trace.QueryDoDoneInfo,
			) {
				start := time.Now()

				return func(info trace.QueryDoDoneInfo) {
					if doConfig.Details()&trace.QueryEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						attempts.With(nil).Record(float64(info.Attempts))
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
		{
			doTxConfig := doConfig.WithSystem("tx")
			errs := doTxConfig.CounterVec("errs", "status")
			attempts := doTxConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
			latency := doTxConfig.TimerVec("latency")
			t.OnDoTx = func(
				info trace.QueryDoTxStartInfo,
			) func(
				trace.QueryDoTxDoneInfo,
			) {
				start := time.Now()

				return func(info trace.QueryDoTxDoneInfo) {
					if doTxConfig.Details()&trace.QueryEvents != 0 {
						attempts.With(nil).Record(float64(info.Attempts))
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
	}
	{
		sessionConfig := queryConfig.WithSystem("session")
		count := sessionConfig.GaugeVec("count")
		{
			createConfig := sessionConfig.WithSystem("create")
			errs := createConfig.CounterVec("errs", "status")
			latency := createConfig.TimerVec("latency")
			t.OnSessionCreate = func(
				info trace.QuerySessionCreateStartInfo,
			) func(
				info trace.QuerySessionCreateDoneInfo,
			) {
				start := time.Now()

				return func(info trace.QuerySessionCreateDoneInfo) {
					if createConfig.Details()&trace.QuerySessionEvents != 0 {
						if info.Error == nil {
							count.With(nil).Add(1)
						}
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
					}
					latency.With(nil).Record(time.Since(start))
				}
			}
		}
		{
			deleteConfig := sessionConfig.WithSystem("delete")
			errs := deleteConfig.CounterVec("errs", "status")
			latency := deleteConfig.TimerVec("latency")
			t.OnSessionDelete = func(info trace.QuerySessionDeleteStartInfo) func(info trace.QuerySessionDeleteDoneInfo) {
				count.With(nil).Add(-1)

				start := time.Now()

				return func(info trace.QuerySessionDeleteDoneInfo) {
					if deleteConfig.Details()&trace.QuerySessionEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
		{
			executeConfig := sessionConfig.WithSystem("execute")
			errs := executeConfig.CounterVec("errs", "status")
			latency := executeConfig.TimerVec("latency")
			t.OnSessionExecute = func(info trace.QuerySessionExecuteStartInfo) func(info trace.QuerySessionExecuteDoneInfo) {
				start := time.Now()

				return func(info trace.QuerySessionExecuteDoneInfo) {
					if executeConfig.Details()&trace.QuerySessionEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
		{
			beginConfig := sessionConfig.WithSystem("begin")
			errs := beginConfig.CounterVec("errs", "status")
			latency := beginConfig.TimerVec("latency")
			t.OnSessionBegin = func(info trace.QuerySessionBeginStartInfo) func(info trace.QuerySessionBeginDoneInfo) {
				start := time.Now()

				return func(info trace.QuerySessionBeginDoneInfo) {
					if beginConfig.Details()&trace.QuerySessionEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
	}
	{
		txConfig := queryConfig.WithSystem("tx")
		{
			executeConfig := txConfig.WithSystem("execute")
			errs := executeConfig.CounterVec("errs", "status")
			latency := executeConfig.TimerVec("latency")
			t.OnTxExecute = func(info trace.QueryTxExecuteStartInfo) func(info trace.QueryTxExecuteDoneInfo) {
				start := time.Now()

				return func(info trace.QueryTxExecuteDoneInfo) {
					if executeConfig.Details()&trace.QuerySessionEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
	}

	return t
}
