package metrics

import (
	"strconv"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// driver makes driver with New publishing
//
//nolint:funlen
func driver(config Config) (t trace.Driver) {
	config.GaugeVec("info", "version").With(map[string]string{"version": version.Version}).Set(1)
	config = config.WithSystem("driver")
	endpoints := config.WithSystem("balancer").GaugeVec("endpoints", "az")
	balancersDiscoveries := config.WithSystem("balancer").CounterVec("discoveries", "status", "cause")
	balancerUpdates := config.WithSystem("balancer").CounterVec("updates", "cause")
	conns := config.GaugeVec("conns", "endpoint", "node_id")
	banned := config.WithSystem("conn").GaugeVec("banned", "endpoint", "node_id", "cause")
	requestStatuses := config.WithSystem("conn").CounterVec("request_statuses", "status", "endpoint", "node_id")
	requestMethods := config.WithSystem("conn").CounterVec("request_methods", "method", "endpoint", "node_id")
	tli := config.CounterVec("transaction_locks_invalidated")

	type endpointKey struct {
		az string
	}
	knownEndpoints := make(map[endpointKey]struct{})
	endpointsMu := sync.RWMutex{}

	t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
		var (
			method   = info.Method
			endpoint = info.Endpoint.Address()
			nodeID   = info.Endpoint.NodeID()
		)

		return func(info trace.DriverConnInvokeDoneInfo) {
			if config.Details()&trace.DriverConnEvents != 0 {
				requestStatuses.With(map[string]string{
					"status":   errorBrief(info.Error),
					"endpoint": endpoint,
					"node_id":  strconv.FormatUint(uint64(nodeID), 10),
				}).Inc()
				requestMethods.With(map[string]string{
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
	t.OnConnNewStream = func(info trace.DriverConnNewStreamStartInfo) func(
		trace.DriverConnNewStreamDoneInfo,
	) {
		var (
			method   = info.Method
			endpoint = info.Endpoint.Address()
			nodeID   = info.Endpoint.NodeID()
		)

		return func(info trace.DriverConnNewStreamDoneInfo) {
			if config.Details()&trace.DriverConnStreamEvents != 0 {
				requestStatuses.With(map[string]string{
					"status":   errorBrief(info.Error),
					"endpoint": endpoint,
					"node_id":  strconv.FormatUint(uint64(nodeID), 10),
				}).Inc()
				requestMethods.With(map[string]string{
					"method":   string(method),
					"endpoint": endpoint,
					"node_id":  strconv.FormatUint(uint64(nodeID), 10),
				}).Inc()
			}
		}
	}
	t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
		if config.Details()&trace.DriverConnEvents != 0 {
			banned.With(map[string]string{
				"endpoint": info.Endpoint.Address(),
				"node_id":  idToString(info.Endpoint.NodeID()),
				"cause":    errorBrief(info.Cause),
			}).Add(1)
		}

		return nil
	}
	t.OnBalancerClusterDiscoveryAttempt = func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
		trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
	) {
		eventType := repeater.EventType(*info.Context)

		return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
			if config.Details()&trace.DriverBalancerEvents != 0 {
				balancersDiscoveries.With(map[string]string{
					"status": errorBrief(info.Error),
					"cause":  eventType,
				}).Inc()
			}
		}
	}
	t.OnBalancerUpdate = func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
		eventType := repeater.EventType(*info.Context)

		return func(info trace.DriverBalancerUpdateDoneInfo) {
			if config.Details()&trace.DriverBalancerEvents != 0 {
				endpointsMu.Lock()
				defer endpointsMu.Unlock()
				balancerUpdates.With(map[string]string{
					"cause": eventType,
				}).Inc()
				newEndpoints := make(map[endpointKey]int, len(info.Endpoints))
				for _, e := range info.Endpoints {
					e := endpointKey{
						az: e.Location(),
					}
					newEndpoints[e]++
				}
				for e := range knownEndpoints {
					if _, has := newEndpoints[e]; !has {
						delete(knownEndpoints, e)
						endpoints.With(map[string]string{
							"az": e.az,
						}).Set(0)
					}
				}
				for e, count := range newEndpoints {
					knownEndpoints[e] = struct{}{}
					endpoints.With(map[string]string{
						"az": e.az,
					}).Set(float64(count))
				}
			}
		}
	}
	t.OnConnDial = func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
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
	t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
		if config.Details()&trace.DriverConnEvents != 0 {
			conns.With(map[string]string{
				"endpoint": info.Endpoint.Address(),
				"node_id":  idToString(info.Endpoint.NodeID()),
			}).Add(-1)
		}

		return nil
	}

	return t
}
