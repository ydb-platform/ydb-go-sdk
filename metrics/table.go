package metrics

import (
	"fmt"
	"sync"
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
	size := config.GaugeVec("size")
	inflight := config.GaugeVec("inflight")
	inflightLatency := config.WithSystem("inflight").TimerVec("latency")
	wait := config.GaugeVec("wait")
	waitLatency := config.WithSystem("wait").TimerVec("latency")
	t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		return func(info trace.TableInitDoneInfo) {
			limit.With(nil).Set(float64(info.Limit))
		}
	}
	t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
		return func(info trace.TableSessionNewDoneInfo) {
			if info.Error == nil && config.Details()&trace.TableSessionEvents != 0 {
				alive.With(map[string]string{
					"node_id": idToString(info.Session.NodeID()),
				}).Add(1)
			}
		}
	}
	t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
		if config.Details()&trace.TableSessionEvents != 0 {
			alive.With(map[string]string{
				"node_id": idToString(info.Session.NodeID()),
			}).Add(-1)
		}

		return nil
	}
	t.OnPoolSessionAdd = func(info trace.TablePoolSessionAddInfo) {
		if config.Details()&trace.TablePoolEvents != 0 {
			size.With(nil).Add(1)
		}
	}
	t.OnPoolSessionRemove = func(info trace.TablePoolSessionRemoveInfo) {
		if config.Details()&trace.TablePoolEvents != 0 {
			size.With(nil).Add(-1)
		}
	}
	var inflightStarts sync.Map
	t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
		wait.With(nil).Add(1)
		start := time.Now()

		return func(info trace.TablePoolGetDoneInfo) {
			wait.With(nil).Add(-1)
			if info.Error == nil && config.Details()&trace.TablePoolEvents != 0 {
				inflight.With(nil).Add(1)
				inflightStarts.Store(info.Session.ID(), time.Now())
				waitLatency.With(nil).Record(time.Since(start))
			}
		}
	}
	t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
		if config.Details()&trace.TablePoolEvents != 0 {
			inflight.With(nil).Add(-1)
			start, ok := inflightStarts.LoadAndDelete(info.Session.ID())
			if !ok {
				panic(fmt.Sprintf("unknown session '%s'", info.Session.ID()))
			}
			val, ok := start.(time.Time)
			if !ok {
				panic(fmt.Sprintf("unsupported type conversion from %T to time.Time", val))
			}
			inflightLatency.With(nil).Record(time.Since(val))
		}

		return nil
	}
	{
		latency := session.WithSystem("query").TimerVec("latency")
		errs := session.WithSystem("query").CounterVec("errs", "status")
		attempts := session.WithSystem("query").HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10})
		t.OnDo = func(info trace.TableDoStartInfo) func(trace.TableDoDoneInfo) {
			start := time.Now()

			return func(doneInfo trace.TableDoDoneInfo) {
				if config.Details()&trace.TableSessionQueryEvents != 0 {
					latency.With(nil).Record(time.Since(start))
					errs.With(map[string]string{
						"status": errorBrief(doneInfo.Error),
					})
					attempts.With(nil).Record(float64(doneInfo.Attempts))
				}
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
				if config.Details()&trace.TableSessionQueryEvents != 0 {
					latency.With(nil).Record(time.Since(start))
					errs.With(map[string]string{
						"status": errorBrief(doneInfo.Error),
					})
					attempts.With(nil).Record(float64(doneInfo.Attempts))
				}
			}
		}
	}

	return t
}
