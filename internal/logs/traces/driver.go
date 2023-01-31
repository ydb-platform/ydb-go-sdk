package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with logging events from details
func Driver(l logs.Logger, details trace.Details) (t trace.Driver) {
	if details&trace.DriverEvents == 0 {
		return
	}
	ll := newLogger(l, "driver")
	if details&trace.DriverResolverEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("resolver")
		t.OnResolve = func(
			info trace.DriverResolveStartInfo,
		) func(
			trace.DriverResolveDoneInfo,
		) {
			target := info.Target
			addresses := info.Resolved
			ll.Trace("update start",
				logs.String("target", target),
				logs.Strings("resolved", addresses),
			)
			return func(info trace.DriverResolveDoneInfo) {
				if info.Error == nil {
					ll.Info("update done",
						logs.String("target", target),
						logs.Strings("resolved", addresses),
					)
				} else {
					ll.Warn("update failed",
						logs.String("target", target),
						logs.Strings("resolved", addresses),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DriverNetEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("net")
		t.OnNetRead = func(info trace.DriverNetReadStartInfo) func(trace.DriverNetReadDoneInfo) {
			address := info.Address
			ll.Trace("read start",
				logs.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetReadDoneInfo) {
				if info.Error == nil {
					ll.Trace("read done",
						latency(start),
						logs.String("address", address),
						logs.Int("received", info.Received),
					)
				} else {
					ll.Warn("read failed",
						latency(start),
						logs.String("address", address),
						logs.Int("received", info.Received),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnNetWrite = func(info trace.DriverNetWriteStartInfo) func(trace.DriverNetWriteDoneInfo) {
			address := info.Address
			ll.Trace("write start",
				logs.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetWriteDoneInfo) {
				if info.Error == nil {
					ll.Trace("write done",
						latency(start),
						logs.String("address", address),
						logs.Int("sent", info.Sent),
					)
				} else {
					ll.Trace("write failed",
						latency(start),
						logs.String("address", address),
						logs.Int("sent", info.Sent),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			address := info.Address
			ll.Debug("dial start",
				logs.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetDialDoneInfo) {
				if info.Error == nil {
					ll.Debug("dial done",
						latency(start),
						logs.String("address", address),
					)
				} else {
					ll.Error("dial failed",
						latency(start),
						logs.String("address", address),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnNetClose = func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			address := info.Address
			ll.Debug("close start",
				logs.String("address", address),
			)
			start := time.Now()
			return func(info trace.DriverNetCloseDoneInfo) {
				if info.Error == nil {
					ll.Debug("close done",
						latency(start),
						logs.String("address", address),
					)
				} else {
					ll.Warn("close failed",
						latency(start),
						logs.String("address", address),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
	}
	if details&trace.DriverEvents != 0 {
		t.OnInit = func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			endpoint := info.Endpoint
			database := info.Database
			secure := info.Secure
			ll.Info("init start",
				version(),
				logs.String("endpoint", endpoint),
				logs.String("database", database),
				logs.Bool("secure", secure),
			)
			start := time.Now()
			return func(info trace.DriverInitDoneInfo) {
				if info.Error == nil {
					ll.Info("init done",
						logs.String("endpoint", endpoint),
						logs.String("database", database),
						logs.Bool("secure", secure),
						latency(start),
					)
				} else {
					ll.Warn("init failed",
						logs.String("endpoint", endpoint),
						logs.String("database", database),
						logs.Bool("secure", secure),
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnClose = func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			ll.Info("close start")
			start := time.Now()
			return func(info trace.DriverCloseDoneInfo) {
				if info.Error == nil {
					ll.Info("close done",
						latency(start),
					)
				} else {
					ll.Warn("close failed",
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DriverConnEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("conn")
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
			endpoint := info.Endpoint
			ll.Trace("take start",
				logs.Stringer("endpoint", endpoint),
			)
			start := time.Now()
			return func(info trace.DriverConnTakeDoneInfo) {
				if info.Error == nil {
					ll.Trace("take done",
						logs.Stringer("endpoint", endpoint),
						latency(start),
					)
				} else {
					ll.Warn("take failed",
						logs.Stringer("endpoint", endpoint),
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			endpoint := info.Endpoint
			ll.Trace("conn state change start",
				logs.Stringer("endpoint", endpoint),
				logs.Stringer("state", info.State),
			)
			start := time.Now()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				ll.Trace("conn state change done",
					logs.Stringer("endpoint", endpoint),
					latency(start),
					logs.Stringer("state", info.State),
				)
			}
		}
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			endpoint := info.Endpoint
			ll.Trace("conn park start",
				logs.Stringer("endpoint", endpoint),
			)
			start := time.Now()
			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error == nil {
					ll.Trace("conn park done",
						logs.Stringer("endpoint", endpoint),
						latency(start),
					)
				} else {
					ll.Warn("conn park failed",
						logs.Stringer("endpoint", endpoint),
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			endpoint := info.Endpoint
			ll.Trace("conn close start",
				logs.Stringer("endpoint", endpoint),
			)
			start := time.Now()
			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					ll.Trace("conn close done",
						logs.Stringer("endpoint", endpoint),
						latency(start),
					)
				} else {
					ll.Warn("conn close failed",
						logs.Stringer("endpoint", endpoint),
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			endpoint := info.Endpoint
			method := string(info.Method)
			ll.Trace("invoke start",
				logs.Stringer("endpoint", endpoint),
				logs.String("method", method),
			)
			start := time.Now()
			return func(info trace.DriverConnInvokeDoneInfo) {
				if info.Error == nil {
					ll.Trace("invoke done",
						logs.Stringer("endpoint", endpoint),
						logs.String("method", method),
						latency(start),
						logs.Stringer("metadata", metadata(info.Metadata)),
					)
				} else {
					ll.Warn("invoke failed",
						logs.Stringer("endpoint", endpoint),
						logs.String("method", method),
						latency(start),
						logs.Error(info.Error),
						logs.Stringer("metadata", metadata(info.Metadata)),
						version(),
					)
				}
			}
		}
		t.OnConnNewStream = func(
			info trace.DriverConnNewStreamStartInfo,
		) func(
			trace.DriverConnNewStreamRecvInfo,
		) func(
			trace.DriverConnNewStreamDoneInfo,
		) {
			endpoint := info.Endpoint
			method := string(info.Method)
			ll.Trace("streaming start",
				logs.Stringer("endpoint", endpoint),
				logs.String("method", method),
			)
			start := time.Now()
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				if info.Error == nil {
					ll.Trace("streaming intermediate receive",
						logs.Stringer("endpoint", endpoint),
						logs.String("method", method),
						latency(start),
					)
				} else {
					ll.Warn("streaming intermediate fail",
						logs.Stringer("endpoint", endpoint),
						logs.String("method", method),
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
				return func(info trace.DriverConnNewStreamDoneInfo) {
					if info.Error == nil {
						ll.Trace("streaming done",
							logs.Stringer("endpoint", endpoint),
							logs.String("method", method),
							latency(start),
							logs.Stringer("metadata", metadata(info.Metadata)),
						)
					} else {
						ll.Warn("streaming failed",
							logs.Stringer("endpoint", endpoint),
							logs.String("method", method),
							latency(start),
							logs.Error(info.Error),
							logs.Stringer("metadata", metadata(info.Metadata)),
							version(),
						)
					}
				}
			}
		}
		t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			endpoint := info.Endpoint
			ll.Warn("conn.Conn ban start",
				logs.Stringer("endpoint", endpoint),
				logs.NamedError("cause", info.Cause),
				version(),
			)
			start := time.Now()
			return func(info trace.DriverConnBanDoneInfo) {
				ll.Warn("conn.Conn ban done",
					logs.Stringer("endpoint", endpoint),
					latency(start),
					logs.Stringer("state", info.State),
					version(),
				)
			}
		}
		t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
			endpoint := info.Endpoint
			ll.Info("conn.Conn allow start",
				logs.Stringer("endpoint", endpoint),
				version(),
			)

			start := time.Now()
			return func(info trace.DriverConnAllowDoneInfo) {
				ll.Info("conn.Conn allow done",
					logs.Stringer("endpoint", endpoint),
					latency(start),
					logs.Stringer("state", info.State),
					version(),
				)
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("repeater")
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			name := info.Name
			event := info.Event
			ll.Trace("repeater wake up",
				logs.String("name", name),
				logs.String("event", event),
			)
			start := time.Now()
			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				if info.Error == nil {
					ll.Trace("repeater wake up done",
						logs.String("name", name),
						logs.String("event", event),
						latency(start),
					)
				} else {
					ll.Error("repeater wake up failed",
						logs.String("name", name),
						logs.String("event", event),
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DriverBalancerEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("balancer")
		t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			ll.Trace("init start")
			start := time.Now()
			return func(info trace.DriverBalancerInitDoneInfo) {
				ll.Debug("init done",
					latency(start),
				)
			}
		}
		t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
			ll.Trace("close start")
			start := time.Now()
			return func(info trace.DriverBalancerCloseDoneInfo) {
				if info.Error == nil {
					ll.Trace("close done",
						latency(start),
					)
				} else {
					ll.Error("close failed",
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnBalancerChooseEndpoint = func(
			info trace.DriverBalancerChooseEndpointStartInfo,
		) func(
			trace.DriverBalancerChooseEndpointDoneInfo,
		) {
			ll.Trace("select endpoint start")
			start := time.Now()
			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error == nil {
					ll.Trace("select endpoint done",
						latency(start),
						logs.Stringer("endpoint", info.Endpoint),
					)
				} else {
					ll.Warn("select endpoint failed",
						latency(start),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
		t.OnBalancerUpdate = func(
			info trace.DriverBalancerUpdateStartInfo,
		) func(
			trace.DriverBalancerUpdateDoneInfo,
		) {
			ll.Trace("balancer discovery start",
				logs.Bool("needLocalDC", info.NeedLocalDC),
			)
			start := time.Now()
			return func(info trace.DriverBalancerUpdateDoneInfo) {
				if info.Error == nil {
					ll.Info("balancer discovery done",
						latency(start),
						logs.Stringer("endpoints", endpoints(info.Endpoints)),
						logs.String("detectedLocalDC", info.LocalDC),
					)
				} else {
					ll.Error("balancer discovery failed",
						latency(start),
						logs.Error(info.Error),
					)
				}
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("credentials")
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			ll.Trace("get start")
			start := time.Now()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error == nil {
					ll.Trace("get done",
						latency(start),
						logs.String("token", Secret(info.Token)),
					)
				} else {
					ll.Error("get done",
						latency(start),
						logs.String("token", Secret(info.Token)),
						logs.Error(info.Error),
						version(),
					)
				}
			}
		}
	}
	return t
}
