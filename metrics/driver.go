package metrics

import (
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// endpointKey represents a key for mapping endpoints to their properties.
type endpointKey struct {
	localDC bool   // a boolean indicating if the endpoint is in the local data center
	az      string // a string representing the availability zone of the endpoint
}

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

	knownEndpoints := make(map[endpointKey]struct{})
	driverConnEvents := config.Details() & trace.DriverConnEvents
	driverBalancerEvents := config.Details() & trace.DriverBalancerEvents

	t.OnConnInvoke = connInvoke(driverConnEvents, requests, tli)
	t.OnConnNewStream = connNewStream(driverConnEvents, requests)
	t.OnConnBan = connBan(driverConnEvents, banned)
	t.OnBalancerClusterDiscoveryAttempt = balancerClusterDiscoveryAttempt(balancersDiscoveries)
	t.OnBalancerUpdate = balancerUpdate(driverBalancerEvents, balancerUpdates, knownEndpoints, endpoints)
	t.OnConnDial = connDial(driverConnEvents, conns)
	t.OnConnClose = connClose(driverConnEvents, conns)

	return t
}

// connInvoke is a function that returns a callback function to be called
// when a driver connection invoke starts and when it is done.
func connInvoke(
	driverConnEvents trace.Details,
	requests CounterVec,
	tli CounterVec,
) func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
	return func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
		var (
			method   = info.Method
			endpoint = info.Endpoint.Address()
			nodeID   = info.Endpoint.NodeID()
		)

		return func(info trace.DriverConnInvokeDoneInfo) {
			if driverConnEvents != 0 {
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

// connNewStream receives the `driverConnEvents` and `requests` parameters and returns a closure function.
func connNewStream(
	driverConnEvents trace.Details,
	requests CounterVec,
) func(
	info trace.DriverConnNewStreamStartInfo,
) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
	return func(info trace.DriverConnNewStreamStartInfo) func(
		trace.DriverConnNewStreamRecvInfo,
	) func(
		trace.DriverConnNewStreamDoneInfo,
	) {
		var (
			method   = info.Method
			endpoint = info.Endpoint.Address()
			nodeID   = info.Endpoint.NodeID()
		)

		return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
			return func(info trace.DriverConnNewStreamDoneInfo) {
				if driverConnEvents != 0 {
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

// connBan is a function that returns a closure wrapping the logic for tracing a connection ban event.
func connBan(
	driverConnEvents trace.Details,
	banned GaugeVec,
) func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
	return func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
		if driverConnEvents != 0 {
			banned.With(map[string]string{
				"endpoint": info.Endpoint.Address(),
				"node_id":  idToString(info.Endpoint.NodeID()),
				"cause":    errorBrief(info.Cause),
			}).Add(1)
		}

		return nil
	}
}

// balancerClusterDiscoveryAttempt performs balancer cluster discovery attempt.
func balancerClusterDiscoveryAttempt(
	balancersDiscoveries CounterVec,
) func(
	info trace.DriverBalancerClusterDiscoveryAttemptStartInfo,
) func(trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
	return func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
		trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
	) {
		eventType := repeater.EventType(*info.Context)

		return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
			balancersDiscoveries.With(map[string]string{
				"status": errorBrief(info.Error),
				"cause":  eventType,
			}).Inc()
		}
	}
}

// balancerUpdate updates the balancer with new endpoint information.
func balancerUpdate(
	driverBalancerEvents trace.Details,
	balancerUpdates CounterVec,
	knownEndpoints map[endpointKey]struct{},
	endpoints GaugeVec,
) func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
	return func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
		eventType := repeater.EventType(*info.Context)

		return func(info trace.DriverBalancerUpdateDoneInfo) {
			if driverBalancerEvents != 0 {
				balancerUpdates.With(map[string]string{
					"cause": eventType,
				}).Inc()
				newEndpoints := make(map[endpointKey]int, len(info.Endpoints))
				for _, e := range info.Endpoints {
					e := endpointKey{
						localDC: e.LocalDC(),
						az:      e.Location(),
					}
					newEndpoints[e]++
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

// connDial is a function that returns a closure function to handle the event when a driver connection dialing starts
// and completes.
// The closure function takes in information about the dialing start and completion, and updates the conns gauge
// accordingly.
// The closure function is returned and used as the `OnConnDial` callback in the driver configuration.
func connDial(
	driverConnEvents trace.Details,
	conns GaugeVec,
) func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
	return func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
		endpoint := info.Endpoint.Address()
		nodeID := info.Endpoint.NodeID()

		return func(info trace.DriverConnDialDoneInfo) {
			if driverConnEvents != 0 {
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

// connClose is a function that returns a closure accepting a trace.DriverConnCloseStartInfo parameter
// and returns a closure accepting a trace.DriverConnCloseDoneInfo parameter.
// It is used to update the connection gauge when a connection is closed.
func connClose(
	driverConnEvents trace.Details,
	conns GaugeVec,
) func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
	return func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
		if driverConnEvents != 0 {
			conns.With(map[string]string{
				"endpoint": info.Endpoint.Address(),
				"node_id":  idToString(info.Endpoint.NodeID()),
			}).Add(-1)
		}

		return nil
	}
}
