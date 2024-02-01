package metrics

import (
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// driver makes driver with New publishing
func driver(config Config) (t trace.Driver) {
	config = config.WithSystem("driver")
	endpoints := config.WithSystem("balancer").GaugeVec("endpoints", "local_dc", "az")
	balancersDiscoveries := config.WithSystem("balancer").CounterVec("discoveries", "status", "cause")
	balancerUpdates := config.WithSystem("balancer").CounterVec("updates", "cause")
	conns := config.GaugeVec("conns", "endpoint", "node_id")
	banned := config.WithSystem("conn").GaugeVec("banned", "endpoint", "node_id", "cause")
	requests := config.WithSystem("conn").CounterVec("requests", "status", "method", "endpoint", "node_id")
	tli := config.CounterVec("transaction_locks_invalidated")

	t = setupTraceHandlers(config, endpoints, balancersDiscoveries, balancerUpdates, conns, banned, requests, tli)
	return t
}

func setupTraceHandlers(config Config, endpoints GaugeVec, balancersDiscoveries CounterVec, balancerUpdates CounterVec, conns GaugeVec, banned GaugeVec, requests CounterVec, tli CounterVec) trace.Driver {
	var t trace.Driver

	t.OnConnInvoke = setupOnConnInvoke(config, requests, tli)
	t.OnConnNewStream = setupOnConnNewStream(config, requests)
	t.OnConnBan = setupOnConnBan(config, banned)
	t.OnBalancerClusterDiscoveryAttempt = setupOnBalancerClusterDiscoveryAttempt(balancersDiscoveries)
	t.OnBalancerUpdate = setupOnBalancerUpdate(config, balancerUpdates, endpoints)
	t.OnConnDial = setupOnConnDial(config, conns)
	t.OnConnClose = setupOnConnClose(config, conns)

	return t
}

func setupOnConnInvoke(config Config, requests, tli CounterVec) func(trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
	return func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
		var (
			method   = info.Method
			endpoint = info.Endpoint.Address()
			nodeID   = info.Endpoint.NodeID()
		)

		return func(info trace.DriverConnInvokeDoneInfo) {
			if config.Details()&trace.DriverConnEvents != 0 {
				requests.With(map[string]string{
					"status":   errorBrief(info.Error),
					"method":   string(method),
					"endpoint": endpoint,
					"node_id":  strconv.FormatUint(uint64(nodeID), 10),
				}).Inc()
				if xerrors.IsOperationErrorTransactionLocksInvalidated(info.Error) {
					tli.With(nil).Inc()
				}
			}
		}
	}
}

func setupOnConnNewStream(config Config, requests CounterVec) func(trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
	return func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
		var (
			method   = info.Method
			endpoint = info.Endpoint.Address()
			nodeID   = info.Endpoint.NodeID()
		)

		return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
			return func(info trace.DriverConnNewStreamDoneInfo) {
				if config.Details()&trace.DriverConnEvents != 0 {
					requests.With(map[string]string{
						"status":   errorBrief(info.Error),
						"method":   string(method),
						"endpoint": endpoint,
						"node_id":  strconv.FormatUint(uint64(nodeID), 10),
					}).Inc()
				}
			}
		}
	}
}

func setupOnConnBan(config Config, banned GaugeVec) func(trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
	return func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
		if config.Details()&trace.DriverConnEvents != 0 {
			banned.With(map[string]string{
				"endpoint": info.Endpoint.Address(),
				"node_id":  idToString(info.Endpoint.NodeID()),
				"cause":    errorBrief(info.Cause),
			}).Add(1)
		}
		return nil
	}
}

func setupOnBalancerClusterDiscoveryAttempt(balancersDiscoveries CounterVec) func(trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
	return func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
		eventType := repeater.EventType(*info.Context)

		return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
			balancersDiscoveries.With(map[string]string{
				"status": errorBrief(info.Error),
				"cause":  eventType,
			}).Inc()
		}
	}
}

func setupOnBalancerUpdate(config Config, balancerUpdates CounterVec, endpoints GaugeVec) func(trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
	return func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
		eventType := repeater.EventType(*info.Context)

		return func(info trace.DriverBalancerUpdateDoneInfo) {
			if config.Details()&trace.DriverBalancerEvents != 0 {
				balancerUpdates.With(map[string]string{
					"cause": eventType,
				}).Inc()

				type endpointKey struct {
					localDC bool
					az      string
				}
				knownEndpoints := make(map[endpointKey]struct{})

				newEndpoints := make(map[endpointKey]int, len(info.Endpoints))
				for _, e := range info.Endpoints {
					eKey := endpointKey{
						localDC: e.LocalDC(),
						az:      e.Location(),
					}
					newEndpoints[eKey]++
				}
				for e := range knownEndpoints {
					if _, has := newEndpoints[e]; !has {
						delete(knownEndpoints, e)
						endpoints.With(map[string]string{
							"local_dc": strconv.FormatBool(e.localDC),
							"az":       e.az,
						}).Set(0)
					}
				}
				for e, count := range newEndpoints {
					knownEndpoints[e] = struct{}{}
					endpoints.With(map[string]string{
						"local_dc": strconv.FormatBool(e.localDC),
						"az":       e.az,
					}).Set(float64(count))
				}
			}
		}
	}
}

func setupOnConnDial(config Config, conns GaugeVec) func(trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
	return func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
		endpoint := info.Endpoint.Address()
		nodeID := info.Endpoint.NodeID()

		return func(info trace.DriverConnDialDoneInfo) {
			if config.Details()&trace.DriverConnEvents != 0 {
				if info.Error == nil {
					conns.With(map[string]string{
						"endpoint": endpoint,
						"node_id":  idToString(nodeID),
					}).Add(1)
				}
			}
		}
	}
}

func setupOnConnClose(config Config, conns GaugeVec) func(trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
	return func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
		if config.Details()&trace.DriverConnEvents != 0 {
			conns.With(map[string]string{
				"endpoint": info.Endpoint.Address(),
				"node_id":  idToString(info.Endpoint.NodeID()),
			}).Add(-1)
		}
		return nil
	}
}
