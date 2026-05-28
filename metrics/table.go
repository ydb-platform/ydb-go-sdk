package metrics

import (
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:funlen
func table(config Config) (t trace.Table) {
	config = config.WithSystem("table")
	alive := config.GaugeVec("sessions", "node_id")
	session := config.WithSystem("session")
	config = config.WithSystem("pool")
	limit := config.GaugeVec("limit")
	index := config.GaugeVec("index")
	concurrency := config.GaugeVec("concurrency")
	idle := config.GaugeVec("idle")
	wait := config.GaugeVec("wait")
	createInProgress := config.GaugeVec("createInProgress")
	inUse := config.GaugeVec("in_use")
	get := config.CounterVec("get")
	put := config.CounterVec("put")
	with := config.GaugeVec("with")
	nodeHint := config.CounterVec("node_hint", "preferred_node_id", "session_node_id", "hit")
	t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		return func(info trace.TableInitDoneInfo) {
			if config.Details()&trace.TableEvents == 0 {
				return
			}

			limit.With(nil).Set(float64(info.Limit))
		}
	}
	t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
		return func(info trace.TableSessionNewDoneInfo) {
			if config.Details()&trace.TableSessionEvents == 0 {
				return
			}

			if info.Error != nil || info.Session == nil {
				return
			}

			alive.With(map[string]string{
				"node_id": idToString(info.Session.NodeID()),
			}).Add(1)
		}
	}
	t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
		if config.Details()&trace.TableSessionEvents == 0 {
			return nil
		}

		alive.With(map[string]string{
			"node_id": idToString(info.Session.NodeID()),
		}).Add(-1)
		return nil
	}
	t.OnPoolWith = func(info trace.TablePoolWithStartInfo) func(trace.TablePoolWithDoneInfo) {
		if config.Details()&trace.TablePoolEvents == 0 {
			return nil
		}

		with.With(nil).Add(1)

		return func(info trace.TablePoolWithDoneInfo) {
			if config.Details()&trace.TablePoolEvents == 0 {
				return
			}
			with.With(nil).Add(-1)
		}
	}
	t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
		return func(info trace.TablePoolGetDoneInfo) {
			if config.Details()&trace.TablePoolEvents == 0 {
				return
			}

			if info.Error == nil {
				get.With(nil).Inc()
			}
			if info.NodeHintInfo != nil {
				preferred := idToString(info.NodeHintInfo.PreferredNodeID)
				actual := idToString(info.NodeHintInfo.SessionNodeID)
				nodeHint.With(map[string]string{
					"preferred_node_id": preferred,
					"session_node_id":   actual,
					"hit":               strconv.FormatBool(preferred == actual),
				}).Inc()
			}
		}
	}
	t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
		if config.Details()&trace.TablePoolEvents == 0 {
			return nil
		}

		put.With(nil).Inc()
		return nil
	}
	t.OnPoolStateChange = func(info trace.TablePoolStateChangeInfo) {
		if config.Details()&trace.TablePoolEvents == 0 {
			return
		}

		limit.With(nil).Set(float64(info.Limit))
		index.With(nil).Set(float64(info.Size))
		concurrency.With(nil).Set(float64(info.Concurrency))
		idle.With(nil).Set(float64(info.Idle))
		wait.With(nil).Set(func() float64 {
			if info.Concurrency > info.Limit {
				return float64(info.Concurrency - info.Limit)
			}

			return 0
		}())
		createInProgress.With(nil).Set(float64(info.CreateInProgress))
		inUse.With(nil).Set(float64(info.Size - info.Idle))
	}
	{
		latency := session.WithSystem("query").TimerVec("latency")
		errs := session.WithSystem("query").CounterVec("errs", "status")
		attempts := session.WithSystem("query").HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
		t.OnDo = func(info trace.TableDoStartInfo) func(trace.TableDoDoneInfo) {
			start := time.Now()

			return func(doneInfo trace.TableDoDoneInfo) {
				if config.Details()&trace.TableSessionQueryEvents == 0 {
					return
				}

				latency.With(nil).Record(time.Since(start))
				errs.With(map[string]string{
					"status": errorBrief(doneInfo.Error),
				})
				attempts.With(nil).Record(float64(doneInfo.Attempts))
			}
		}
	}
	{
		latency := session.WithSystem("tx").TimerVec("latency")
		errs := session.WithSystem("tx").CounterVec("errs", "status")
		attempts := session.WithSystem("tx").HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(trace.TableDoTxDoneInfo) {
			start := time.Now()

			return func(doneInfo trace.TableDoTxDoneInfo) {
				if config.Details()&trace.TableSessionQueryEvents == 0 {
					return
				}

				latency.With(nil).Record(time.Since(start))
				errs.With(map[string]string{
					"status": errorBrief(doneInfo.Error),
				})
				attempts.With(nil).Record(float64(doneInfo.Attempts))
			}
		}
	}

	return t
}
