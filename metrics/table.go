package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func table(config Config) (t trace.Table) {
	config = config.WithSystem("table")
	alive := config.GaugeVec("sessions", "node_id")
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
			inflightLatency.With(nil).Record(time.Since(start.(time.Time)))
		}
		return nil
	}
	return t
}
