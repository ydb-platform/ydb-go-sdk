package metrics

import (
	"strconv"
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
			errs := withConfig.CounterVec("errs", statusLabel)
			attempts := withConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
			latency := withConfig.TimerVec("latency")
			t.OnPoolWith = func(trace.QueryPoolWithStartInfo) func(trace.QueryPoolWithDoneInfo) {
				if withConfig.Details()&trace.QueryPoolEvents == 0 {
					return nil
				}
				start := time.Now()

				return func(info trace.QueryPoolWithDoneInfo) {
					if info.Error != nil {
						errs.With(map[string]string{
							statusLabel: errorBrief(info.Error),
						}).Inc()
					}
					attempts.With(nil).Record(float64(info.Attempts))
					latency.With(nil).Record(time.Since(start))
				}
			}
		}
		{
			nodeHint := poolConfig.CounterVec("node_hint", preferredNodeIDLabel, sessionNodeIDLabel, hitLabel)
			t.OnPoolGet = func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
				return func(info trace.QueryPoolGetDoneInfo) {
					if poolConfig.Details()&trace.QueryPoolEvents == 0 {
						return
					}

					if info.NodeHintInfo == nil {
						return
					}

					preferred := idToString(info.NodeHintInfo.PreferredNodeID)
					actual := idToString(info.NodeHintInfo.SessionNodeID)
					nodeHint.With(map[string]string{
						preferredNodeIDLabel: preferred,
						sessionNodeIDLabel:   actual,
						hitLabel:             strconv.FormatBool(preferred == actual),
					}).Inc()
				}
			}
		}
		{
			sizeConfig := poolConfig.WithSystem("size")
			limit := sizeConfig.GaugeVec("limit")
			idle := sizeConfig.GaugeVec("idle")
			index := sizeConfig.GaugeVec("index")
			wait := sizeConfig.GaugeVec("waiters_queue")
			inUse := sizeConfig.GaugeVec("in_use")
			concurrency := sizeConfig.GaugeVec("concurrency")
			createInProgress := sizeConfig.GaugeVec("create_in_progress")
			t.OnPoolChange = func(stats trace.QueryPoolChange) {
				if sizeConfig.Details()&trace.QueryPoolEvents == 0 {
					return
				}

				limit.With(nil).Set(float64(stats.Limit))
				idle.With(nil).Set(float64(stats.Idle))
				index.With(nil).Set(float64(stats.Size))
				wait.With(nil).Set(func() float64 {
					if stats.Concurrency > stats.Limit {
						return float64(stats.Concurrency - stats.Limit)
					}

					return 0
				}())
				concurrency.With(nil).Set(float64(stats.Concurrency))
				createInProgress.With(nil).Set(float64(stats.CreateInProgress))
				inUse.With(nil).Set(float64(stats.Size - stats.Idle))
			}
		}
	}
	{
		doConfig := queryConfig.WithSystem("do")
		{
			errs := doConfig.CounterVec("errs", statusLabel, operationLabel)
			attempts := doConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10}, operationLabel)
			latency := doConfig.TimerVec("latency", operationLabel)
			t.OnDo = func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QueryDoDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if doConfig.Details()&trace.QueryEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					attempts.With(labels).Record(float64(info.Attempts))
					latency.With(labels).Record(time.Since(start))
				}
			}
		}
		{
			doTxConfig := doConfig.WithSystem("tx")
			errs := doTxConfig.CounterVec("errs", statusLabel, operationLabel)
			attempts := doTxConfig.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10}, operationLabel)
			latency := doTxConfig.TimerVec("latency", operationLabel)
			t.OnDoTx = func(info trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QueryDoTxDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if doTxConfig.Details()&trace.QueryEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					attempts.With(labels).Record(float64(info.Attempts))
					latency.With(labels).Record(time.Since(start))
				}
			}
		}
		{
			doTxConfig := doConfig.WithSystem("exec")
			errs := doTxConfig.CounterVec("errs", statusLabel, operationLabel)
			latency := doTxConfig.TimerVec("latency", operationLabel)
			t.OnExec = func(info trace.QueryExecStartInfo) func(trace.QueryExecDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QueryExecDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if doTxConfig.Details()&trace.QueryEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					latency.With(labels).Record(time.Since(start))
				}
			}
		}
		{
			qqConfig := queryConfig.WithSystem("query")
			errs := qqConfig.CounterVec("errs", statusLabel, operationLabel)
			latency := qqConfig.TimerVec("latency", operationLabel)
			t.OnQuery = func(info trace.QueryQueryStartInfo) func(trace.QueryQueryDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QueryQueryDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if qqConfig.Details()&trace.QueryEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					latency.With(labels).Record(time.Since(start))
				}
			}
			{
				qqConfig := queryConfig.WithSystem("row")
				errs := qqConfig.CounterVec("errs", statusLabel, operationLabel)
				latency := qqConfig.TimerVec("latency", operationLabel)
				t.OnQueryRow = func(info trace.QueryQueryRowStartInfo) func(trace.QueryQueryRowDoneInfo) {
					start := time.Now()
					label := info.Label

					if label == "" {
						return nil
					}

					return func(info trace.QueryQueryRowDoneInfo) {
						labels := map[string]string{operationLabel: label}
						if qqConfig.Details()&trace.QueryEvents == 0 {
							return
						}

						errs.With(map[string]string{
							statusLabel:    errorBrief(info.Error),
							operationLabel: label,
						}).Inc()
						latency.With(labels).Record(time.Since(start))
					}
				}
			}
			{
				qqConfig := queryConfig.WithSystem("result").WithSystem("set")
				errs := qqConfig.CounterVec("errs", statusLabel, operationLabel)
				latency := qqConfig.TimerVec("latency", operationLabel)
				rowsCount := qqConfig.GaugeVec("rows", operationLabel)
				t.OnQueryResultSet = func(info trace.QueryQueryResultSetStartInfo) func(trace.QueryQueryResultSetDoneInfo) {
					start := time.Now()
					label := info.Label

					if label == "" {
						return nil
					}

					return func(info trace.QueryQueryResultSetDoneInfo) {
						labels := map[string]string{operationLabel: label}
						if qqConfig.Details()&trace.QueryEvents == 0 {
							return
						}

						errs.With(map[string]string{
							statusLabel:    errorBrief(info.Error),
							operationLabel: label,
						}).Inc()
						latency.With(labels).Record(time.Since(start))
						if info.Error == nil {
							rowsCount.With(labels).Set(float64(info.RowsCount))
						}
					}
				}
			}
		}
	}
	{
		sessionConfig := queryConfig.WithSystem("session")
		count := sessionConfig.GaugeVec("count")
		closed := sessionConfig.CounterVec("closed", "ydb.query.session.pool.name", "reason")
		t.OnSessionClosed = func(info trace.QuerySessionClosedInfo) {
			if sessionConfig.Details()&trace.QuerySessionEvents == 0 {
				return
			}

			closed.With(map[string]string{
				"ydb.query.session.pool.name": info.PoolName,
				"reason":                      info.Reason,
			}).Inc()
		}
		{
			createConfig := sessionConfig.WithSystem("create")
			errs := createConfig.CounterVec("errs", statusLabel)
			latency := createConfig.TimerVec("latency")
			t.OnSessionCreate = func(trace.QuerySessionCreateStartInfo) func(trace.QuerySessionCreateDoneInfo) {
				start := time.Now()

				return func(info trace.QuerySessionCreateDoneInfo) {
					if createConfig.Details()&trace.QuerySessionEvents == 0 {
						return
					}

					if info.Error == nil && !isNil(info.Session) {
						count.With(nil).Add(1)
					}
					errs.With(map[string]string{
						statusLabel: errorBrief(info.Error),
					}).Inc()
					latency.With(nil).Record(time.Since(start))
				}
			}
		}
		{
			deleteConfig := sessionConfig.WithSystem("delete")
			errs := deleteConfig.CounterVec("errs", statusLabel)
			latency := deleteConfig.TimerVec("latency")
			t.OnSessionDelete = func(trace.QuerySessionDeleteStartInfo) func(trace.QuerySessionDeleteDoneInfo) {
				start := time.Now()

				return func(info trace.QuerySessionDeleteDoneInfo) {
					if deleteConfig.Details()&trace.QuerySessionEvents == 0 {
						return
					}

					count.With(nil).Add(-1)
					errs.With(map[string]string{
						statusLabel: errorBrief(info.Error),
					}).Inc()
					latency.With(nil).Record(time.Since(start))
				}
			}
		}
		{
			sessionExecConfig := sessionConfig.WithSystem("exec")
			errs := sessionExecConfig.CounterVec("errs", statusLabel, operationLabel)
			latency := sessionExecConfig.TimerVec("latency", operationLabel)
			t.OnSessionExec = func(info trace.QuerySessionExecStartInfo) func(trace.QuerySessionExecDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QuerySessionExecDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if sessionExecConfig.Details()&trace.QuerySessionEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					latency.With(labels).Record(time.Since(start))
				}
			}
		}
		{
			sessionQueryConfig := sessionConfig.WithSystem("query")
			errs := sessionQueryConfig.CounterVec("errs", statusLabel, operationLabel)
			latency := sessionQueryConfig.TimerVec("latency", operationLabel)
			t.OnSessionQuery = func(info trace.QuerySessionQueryStartInfo) func(trace.QuerySessionQueryDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QuerySessionQueryDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if sessionQueryConfig.Details()&trace.QuerySessionEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					latency.With(labels).Record(time.Since(start))
				}
			}
		}
		{
			beginConfig := sessionConfig.WithSystem("begin")
			errs := beginConfig.CounterVec("errs", statusLabel)
			latency := beginConfig.TimerVec("latency")
			t.OnSessionBegin = func(trace.QuerySessionBeginStartInfo) func(trace.QuerySessionBeginDoneInfo) {
				start := time.Now()

				return func(info trace.QuerySessionBeginDoneInfo) {
					if beginConfig.Details()&trace.QuerySessionEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel: errorBrief(info.Error),
					}).Inc()
					latency.With(nil).Record(time.Since(start))
				}
			}
		}
	}
	{
		txConfig := queryConfig.WithSystem("tx")
		{
			txExecConfig := txConfig.WithSystem("exec")
			errs := txExecConfig.CounterVec("errs", statusLabel, operationLabel)
			latency := txExecConfig.TimerVec("latency", operationLabel)
			t.OnTxExec = func(info trace.QueryTxExecStartInfo) func(trace.QueryTxExecDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QueryTxExecDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if txExecConfig.Details()&trace.QuerySessionEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					latency.With(labels).Record(time.Since(start))
				}
			}
		}
		{
			txQueryConfig := txConfig.WithSystem("query")
			errs := txQueryConfig.CounterVec("errs", statusLabel, operationLabel)
			latency := txQueryConfig.TimerVec("latency", operationLabel)
			t.OnTxQuery = func(info trace.QueryTxQueryStartInfo) func(trace.QueryTxQueryDoneInfo) {
				start := time.Now()
				label := info.Label

				return func(info trace.QueryTxQueryDoneInfo) {
					labels := map[string]string{operationLabel: label}
					if txQueryConfig.Details()&trace.QuerySessionEvents == 0 {
						return
					}

					errs.With(map[string]string{
						statusLabel:    errorBrief(info.Error),
						operationLabel: label,
					}).Inc()
					latency.With(labels).Record(time.Since(start))
				}
			}
			{
				txQueryConfig := txConfig.WithSystem("row")
				errs := txQueryConfig.CounterVec("errs", statusLabel, operationLabel)
				latency := txQueryConfig.TimerVec("latency", operationLabel)
				t.OnTxQueryRow = func(info trace.QueryTxQueryRowStartInfo) func(trace.QueryTxQueryRowDoneInfo) {
					start := time.Now()
					label := info.Label

					return func(info trace.QueryTxQueryRowDoneInfo) {
						labels := map[string]string{operationLabel: label}
						if txQueryConfig.Details()&trace.QuerySessionEvents == 0 {
							return
						}

						errs.With(map[string]string{
							statusLabel:    errorBrief(info.Error),
							operationLabel: label,
						}).Inc()
						latency.With(labels).Record(time.Since(start))
					}
				}
			}
			{
				txQueryConfig := txConfig.WithSystem("result").WithSystem("set")
				errs := txQueryConfig.CounterVec("errs", statusLabel, operationLabel)
				latency := txQueryConfig.TimerVec("latency", operationLabel)
				t.OnTxQueryResultSet = func(info trace.QueryTxQueryResultSetStartInfo) func(trace.QueryTxQueryResultSetDoneInfo) {
					start := time.Now()
					label := info.Label

					return func(info trace.QueryTxQueryResultSetDoneInfo) {
						labels := map[string]string{operationLabel: label}
						if txQueryConfig.Details()&trace.QuerySessionEvents == 0 {
							return
						}

						errs.With(map[string]string{
							statusLabel:    errorBrief(info.Error),
							operationLabel: label,
						}).Inc()
						latency.With(labels).Record(time.Since(start))
					}
				}
			}
		}
	}

	return t
}
