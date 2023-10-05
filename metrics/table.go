package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func table(config Config) (t trace.Table) {
	config = config.WithSystem("table")
	limit := config.WithSystem("pool").GaugeVec("limit")
	size := config.WithSystem("pool").GaugeVec("size")
	inflight := config.WithSystem("pool").GaugeVec("inflight")
	inflightLatency := config.WithSystem("pool").WithSystem("inflight").TimerVec("latency")
	wait := config.WithSystem("pool").GaugeVec("wait")
	waitLatency := config.WithSystem("pool").WithSystem("wait").TimerVec("latency")
	alive := config.GaugeVec("sessions", "node_id")
	doAttempts := config.WithSystem("do").HistogramVec("attempts", []float64{0, 1, 2, 5, 10}, "name")
	doErrors := config.WithSystem("do").CounterVec("errors", "status", "name")
	doIntermediateErrors := config.WithSystem("do").WithSystem("intermediate").CounterVec("errors", "status", "name")
	doLatency := config.WithSystem("do").TimerVec("latency", "status", "name")
	doTxAttempts := config.WithSystem("doTx").HistogramVec("attempts", []float64{0, 1, 2, 5, 10}, "name")
	doTxIntermediateErrors := config.WithSystem("doTx").WithSystem("intermediate").CounterVec("errors", "status", "name")
	doTxErrors := config.WithSystem("doTx").CounterVec("errors", "status", "name")
	doTxLatency := config.WithSystem("doTx").TimerVec("latency", "status", "name")
	t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		return func(info trace.TableInitDoneInfo) {
			limit.With(nil).Set(float64(info.Limit))
		}
	}
	t.OnDo = func(info trace.TableDoStartInfo) func(
		info trace.TableDoIntermediateInfo,
	) func(
		trace.TableDoDoneInfo,
	) {
		var (
			name  = info.ID
			start = time.Now()
		)
		return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			if info.Error != nil && config.Details()&trace.TableEvents != 0 {
				doIntermediateErrors.With(map[string]string{
					"status": errorBrief(info.Error),
					"name":   name,
				}).Inc()
			}
			return func(info trace.TableDoDoneInfo) {
				if config.Details()&trace.TableEvents != 0 {
					doAttempts.With(nil).Record(float64(info.Attempts))
					doErrors.With(map[string]string{
						"status": errorBrief(info.Error),
						"name":   name,
					}).Inc()
					doLatency.With(map[string]string{
						"status": errorBrief(info.Error),
						"name":   name,
					}).Record(time.Since(start))
				}
			}
		}
	}
	t.OnDoTx = func(info trace.TableDoTxStartInfo) func(
		info trace.TableDoTxIntermediateInfo,
	) func(
		trace.TableDoTxDoneInfo,
	) {
		var (
			name  = info.ID
			start = time.Now()
		)
		return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			if info.Error != nil && config.Details()&trace.TableEvents != 0 {
				doTxIntermediateErrors.With(map[string]string{
					"status": errorBrief(info.Error),
					"name":   name,
				}).Inc()
			}
			return func(info trace.TableDoTxDoneInfo) {
				if config.Details()&trace.TableEvents != 0 {
					doTxAttempts.With(nil).Record(float64(info.Attempts))
					doTxErrors.With(map[string]string{
						"status": errorBrief(info.Error),
						"name":   name,
					}).Inc()
					doTxLatency.With(map[string]string{
						"status": errorBrief(info.Error),
						"name":   name,
					}).Record(time.Since(start))
				}
			}
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
			inflightLatency.With(nil).Record(time.Since(start.(time.Time)))
		}
		return nil
	}
	return t
}
