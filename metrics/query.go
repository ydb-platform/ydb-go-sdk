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
			inUse := sizeConfig.WithSystem("in").GaugeVec("use")

			t.OnPoolChange = func(stats trace.QueryPoolChange) {
				if sizeConfig.Details()&trace.QueryPoolEvents == 0 {
					return
				}

				limit.With(nil).Set(float64(stats.Limit))
				idle.With(nil).Set(float64(stats.Idle))
				inUse.With(nil).Set(float64(stats.InUse))
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
			sessionExecConfig := sessionConfig.WithSystem("exec")
			errs := sessionExecConfig.CounterVec("errs", "status")
			latency := sessionExecConfig.TimerVec("latency")
			t.OnSessionExec = func(info trace.QuerySessionExecStartInfo) func(info trace.QuerySessionExecDoneInfo) {
				start := time.Now()

				return func(info trace.QuerySessionExecDoneInfo) {
					if sessionExecConfig.Details()&trace.QuerySessionEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
		{
			sessionQueryConfig := sessionConfig.WithSystem("query")
			errs := sessionQueryConfig.CounterVec("errs", "status")
			latency := sessionQueryConfig.TimerVec("latency")
			t.OnSessionQuery = func(info trace.QuerySessionQueryStartInfo) func(info trace.QuerySessionQueryDoneInfo) {
				start := time.Now()

				return func(info trace.QuerySessionQueryDoneInfo) {
					if sessionQueryConfig.Details()&trace.QuerySessionEvents != 0 {
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
			txExecConfig := txConfig.WithSystem("exec")
			errs := txExecConfig.CounterVec("errs", "status")
			latency := txExecConfig.TimerVec("latency")
			t.OnTxExec = func(info trace.QueryTxExecStartInfo) func(info trace.QueryTxExecDoneInfo) {
				start := time.Now()

				return func(info trace.QueryTxExecDoneInfo) {
					if txExecConfig.Details()&trace.QuerySessionEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
		{
			txQueryConfig := txConfig.WithSystem("query")
			errs := txQueryConfig.CounterVec("errs", "status")
			latency := txQueryConfig.TimerVec("latency")
			t.OnTxQuery = func(info trace.QueryTxQueryStartInfo) func(info trace.QueryTxQueryDoneInfo) {
				start := time.Now()

				return func(info trace.QueryTxQueryDoneInfo) {
					if txQueryConfig.Details()&trace.QuerySessionEvents != 0 {
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
