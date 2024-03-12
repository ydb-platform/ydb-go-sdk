package metrics

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func query(config Config) (t trace.Query) {
	queryConfig := config.WithSystem("query")
	{
		doConfig := queryConfig.WithSystem("do")
		{
			intermediateErrs := doConfig.WithSystem("intermediate").CounterVec("errs", "status")
			errs := doConfig.CounterVec("errs", "status")
			attempts := doConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
			latency := doConfig.TimerVec("latency")
			t.OnDo = func(
				info trace.QueryDoStartInfo,
			) func(
				info trace.QueryDoIntermediateInfo,
			) func(
				trace.QueryDoDoneInfo,
			) {
				start := time.Now()

				return func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
					if info.Error != nil && doConfig.Details()&trace.QueryEvents != 0 {
						intermediateErrs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
					}

					return func(info trace.QueryDoDoneInfo) {
						if doConfig.Details()&trace.QueryEvents != 0 {
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
			doTxConfig := doConfig.WithSystem("tx")
			intermediateErrs := doTxConfig.WithSystem("intermediate").CounterVec("errs", "status")
			errs := doTxConfig.CounterVec("errs", "status")
			attempts := doTxConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
			latency := doTxConfig.TimerVec("latency")
			t.OnDoTx = func(
				info trace.QueryDoTxStartInfo,
			) func(
				info trace.QueryDoTxIntermediateInfo,
			) func(
				trace.QueryDoTxDoneInfo,
			) {
				start := time.Now()

				return func(info trace.QueryDoTxIntermediateInfo) func(trace.QueryDoTxDoneInfo) {
					if info.Error != nil && doTxConfig.Details()&trace.QueryEvents != 0 {
						intermediateErrs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
					}

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
	}
	{
		sessionConfig := queryConfig.WithSystem("session")
		{
			createConfig := sessionConfig.WithSystem("create")
			errs := createConfig.CounterVec("errs", "status")
			latency := createConfig.TimerVec("latency")
			t.OnCreateSession = func(
				info trace.QueryCreateSessionStartInfo,
			) func(
				info trace.QueryCreateSessionDoneInfo,
			) {
				start := time.Now()

				return func(info trace.QueryCreateSessionDoneInfo) {
					if info.Error != nil && createConfig.Details()&trace.QuerySessionEvents != 0 {
						errs.With(map[string]string{
							"status": errorBrief(info.Error),
						}).Inc()
						latency.With(nil).Record(time.Since(start))
					}
				}
			}
		}
		{
			deleteConfig := sessionConfig.WithSystem("delete")
			errs := deleteConfig.CounterVec("errs", "status")
			latency := deleteConfig.TimerVec("latency")
			t.OnCreateSession = func(
				info trace.QueryCreateSessionStartInfo,
			) func(
				info trace.QueryCreateSessionDoneInfo,
			) {
				start := time.Now()

				return func(info trace.QueryCreateSessionDoneInfo) {
					if info.Error != nil && deleteConfig.Details()&trace.QuerySessionEvents != 0 {
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
