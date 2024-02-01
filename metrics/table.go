package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func table(config Config) (t trace.Table) {
	config = config.WithSystem("table")
	metrics := TableMetrics{
		alive:           config.GaugeVec("sessions", "node_id"),
		limit:           config.GaugeVec("limit"),
		size:            config.GaugeVec("size"),
		inflight:        config.GaugeVec("inflight"),
		inflightLatency: config.WithSystem("inflight").TimerVec("latency"),
		wait:            config.GaugeVec("wait"),
		waitLatency:     config.WithSystem("wait").TimerVec("latency"),
	}

	t = setupTableEventHandlers(config, metrics)
	return t
}

func setupTableEventHandlers(config Config, metrics TableMetrics) trace.Table {
	var t trace.Table
	var inflightStarts sync.Map

	t.OnInit = setupOnInit(metrics.limit)
	t.OnSessionNew = setupOnSessionNew(config, metrics.alive)
	t.OnSessionDelete = setupOnSessionDelete(config, metrics.alive)
	t.OnPoolSessionAdd = setupOnPoolSessionAdd(config, metrics.size)
	t.OnPoolSessionRemove = setupOnPoolSessionRemove(config, metrics.size)
	t.OnPoolGet = setupOnPoolGet(config, metrics.inflight, metrics.inflightLatency, metrics.wait, metrics.waitLatency, &inflightStarts)
	t.OnPoolPut = setupOnPoolPut(config, metrics.inflight, metrics.inflightLatency, &inflightStarts)

	return t
}

func setupOnInit(limit GaugeVec) func(trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
	return func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		return func(info trace.TableInitDoneInfo) {
			limit.With(nil).Set(float64(info.Limit))
		}
	}
}

func setupOnSessionNew(config Config, alive GaugeVec) func(trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
	return func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
		return func(info trace.TableSessionNewDoneInfo) {
			if info.Error == nil && config.Details()&trace.TableSessionEvents != 0 {
				alive.With(map[string]string{
					"node_id": idToString(info.Session.NodeID()),
				}).Add(1)
			}
		}
	}
}

func setupOnSessionDelete(config Config, alive GaugeVec) func(trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
	return func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
		if config.Details()&trace.TableSessionEvents != 0 {
			alive.With(map[string]string{
				"node_id": idToString(info.Session.NodeID()),
			}).Add(-1)
		}
		return nil
	}
}

func setupOnPoolSessionAdd(config Config, size GaugeVec) func(trace.TablePoolSessionAddInfo) {
	return func(info trace.TablePoolSessionAddInfo) {
		if config.Details()&trace.TablePoolEvents != 0 {
			size.With(nil).Add(1)
		}
	}
}

func setupOnPoolSessionRemove(config Config, size GaugeVec) func(trace.TablePoolSessionRemoveInfo) {
	return func(info trace.TablePoolSessionRemoveInfo) {
		if config.Details()&trace.TablePoolEvents != 0 {
			size.With(nil).Add(-1)
		}
	}
}

func setupOnPoolGet(config Config, inflight GaugeVec, inflightLatency TimerVec, wait GaugeVec, waitLatency TimerVec, inflightStarts *sync.Map) func(trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
	return func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
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
}

func setupOnPoolPut(config Config, inflight GaugeVec, inflightLatency TimerVec, inflightStarts *sync.Map) func(trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
	return func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
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
}
