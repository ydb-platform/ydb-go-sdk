package structural

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with logging events from details
func Driver(l Logger, details trace.Details) (t trace.Driver) {
	if details&trace.DriverEvents == 0 {
		return
	}
	l = l.WithName(`driver`)
	if details&trace.DriverResolverEvents != 0 {
		//nolint:govet
		l := l.WithName(`resolver`)
		t.OnResolve = func(
			info trace.DriverResolveStartInfo,
		) func(
			trace.DriverResolveDoneInfo,
		) {
			target := info.Target
			addresses := info.Resolved
			l.Trace().
				String("target", target).
				Strings("resolved", addresses).
				Message("update start")
			return func(info trace.DriverResolveDoneInfo) {
				if info.Error == nil {
					l.Info().
						String("target", target).
						Strings("resolved", addresses).
						Message("update done")
				} else {
					l.Warn().
						String("target", target).
						Strings("resolved", addresses).
						Error(info.Error).
						String("version", meta.Version).
						Message("update failed")
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DriverNetEvents != 0 {
		//nolint:govet
		l := l.WithName(`net`)
		t.OnNetRead = func(info trace.DriverNetReadStartInfo) func(trace.DriverNetReadDoneInfo) {
			address := info.Address
			l.Trace().
				String("address", address).
				Message("read start")
			start := time.Now()
			return func(info trace.DriverNetReadDoneInfo) {
				if info.Error == nil {
					l.Trace().
						Duration("latency", time.Since(start)).
						String("address", address).
						Int("received", info.Received).
						Message("read done")
				} else {
					l.Warn().
						Duration("latency", time.Since(start)).
						String("address", address).
						Int("received", info.Received).
						Error(info.Error).
						String("version", meta.Version).
						Message("read failed")
				}
			}
		}
		t.OnNetWrite = func(info trace.DriverNetWriteStartInfo) func(trace.DriverNetWriteDoneInfo) {
			address := info.Address
			l.Trace().
				String("address", address).
				Message("write start")
			start := time.Now()
			return func(info trace.DriverNetWriteDoneInfo) {
				if info.Error == nil {
					l.Trace().
						Duration("latency", time.Since(start)).
						String("address", address).
						Int("sent", info.Sent).
						Message("write done")
				} else {
					l.Warn().
						Duration("latency", time.Since(start)).
						String("address", address).
						Int("sent", info.Sent).
						Error(info.Error).
						String("version", meta.Version).
						Message("write failed")
				}
			}
		}
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			address := info.Address
			l.Debug().
				String("address", address).
				Message("dial start")
			start := time.Now()
			return func(info trace.DriverNetDialDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						String("address", address).
						Message("dial done")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						String("address", address).
						Error(info.Error).
						String("version", meta.Version).
						Message("dial failed")
				}
			}
		}
		t.OnNetClose = func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			address := info.Address
			l.Debug().
				String("address", address).
				Message("close start")
			start := time.Now()
			return func(info trace.DriverNetCloseDoneInfo) {
				if info.Error == nil {
					l.Debug().
						Duration("latency", time.Since(start)).
						String("address", address).
						Message("close done")
				} else {
					l.Warn().
						Duration("latency", time.Since(start)).
						String("address", address).
						Error(info.Error).
						String("version", meta.Version).
						Message("close failed")
				}
			}
		}
	}
	if details&trace.DriverEvents != 0 {
		t.OnInit = func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			endpoint := info.Endpoint
			database := info.Database
			secure := info.Secure
			l.Info().
				String("version", meta.Version).
				String("endpoint", endpoint).
				String("database", database).
				Bool("secure", secure).
				Message("init start")
			start := time.Now()
			return func(info trace.DriverInitDoneInfo) {
				if info.Error == nil {
					l.Info().
						String("endpoint", endpoint).
						String("database", database).
						Bool("secure", secure).
						Duration("latency", time.Since(start)).
						Message("init done")
				} else {
					l.Warn().
						String("endpoint", endpoint).
						String("database", database).
						Bool("secure", secure).
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("init failed")
				}
			}
		}
		t.OnClose = func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			l.Info().Message(`close start`)
			start := time.Now()
			return func(info trace.DriverCloseDoneInfo) {
				if info.Error == nil {
					l.Info().
						Duration("latency", time.Since(start)).
						Message("close done")
				} else {
					l.Warn().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("close failed")
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DriverConnEvents != 0 {
		//nolint:govet
		l := l.WithName(`conn`)
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
			endpoint := info.Endpoint.String()
			l.Trace().
				String("endpoint", endpoint).
				Message("take start")
			start := time.Now()
			return func(info trace.DriverConnTakeDoneInfo) {
				if info.Error == nil {
					l.Trace().
						String("endpoint", endpoint).
						Duration("latency", time.Since(start)).
						Message("take done")
				} else {
					l.Warn().
						String("endpoint", endpoint).
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("take failed")
				}
			}
		}
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			endpoint := info.Endpoint.String()
			l.Trace().
				String("endpoint", endpoint).
				String("state", info.State.String()).
				Message("conn state change start")
			start := time.Now()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				l.Trace().
					String("endpoint", endpoint).
					Duration("latency", time.Since(start)).
					String("state", info.State.String()).
					Message("conn state change done")
			}
		}
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			endpoint := info.Endpoint
			l.Trace().
				String("endpoint", endpoint.String()).
				Message("conn park start")
			start := time.Now()
			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error == nil {
					l.Trace().
						String("endpoint", endpoint.String()).
						Duration("latency", time.Since(start)).
						Message("conn park done")
				} else {
					l.Warn().
						String("endpoint", endpoint.String()).
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("conn park fail")
				}
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			endpoint := info.Endpoint
			l.Trace().
				String("endpoint", endpoint.String()).
				Message("conn close start")
			start := time.Now()
			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					l.Trace().
						String("endpoint", endpoint.String()).
						Duration("latency", time.Since(start)).
						Message("conn close done")
				} else {
					l.Warn().
						String("endpoint", endpoint.String()).
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("conn close fail")
				}
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			endpoint := info.Endpoint.String()
			method := string(info.Method)
			l.Trace().
				String("endpoint", endpoint).
				String("method", method).
				Message("invoke start")
			start := time.Now()
			return func(info trace.DriverConnInvokeDoneInfo) {
				if info.Error == nil {
					l.Trace().
						String("endpoint", endpoint).
						String("method", method).
						Duration("latency", time.Since(start)).
						Object("metadata", Metadata(info.Metadata, l.Object())).
						Message("invoke done")
				} else {
					l.Warn().
						String("endpoint", endpoint).
						String("method", method).
						Duration("latency", time.Since(start)).
						Error(info.Error).
						Object("metadata", Metadata(info.Metadata, l.Object())).
						String("version", meta.Version).
						Message("invoke failed")
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
			endpoint := info.Endpoint.String()
			method := string(info.Method)
			l.Trace().
				String("endpoint", endpoint).
				String("method", method).
				Message("streaming start")
			start := time.Now()
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				if info.Error == nil {
					l.Trace().
						String("endpoint", endpoint).
						String("method", method).
						Duration("latency", time.Since(start)).
						Message("streaming intermediate receive")
				} else {
					l.Warn().
						String("endpoint", endpoint).
						String("method", method).
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("streaming intermediate fail")
				}
				return func(info trace.DriverConnNewStreamDoneInfo) {
					if info.Error == nil {
						l.Trace().
							String("endpoint", endpoint).
							String("method", method).
							Duration("latency", time.Since(start)).
							Object("metadata", Metadata(info.Metadata, l.Object())).
							Message("streaming done")
					} else {
						l.Warn().
							String("endpoint", endpoint).
							String("method", method).
							Duration("latency", time.Since(start)).
							Error(info.Error).
							Object("metadata", Metadata(info.Metadata, l.Object())).
							String("version", meta.Version).
							Message("streaming done")
					}
				}
			}
		}
		t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			endpoint := info.Endpoint.String()
			l.Warn().
				String("endpoint", endpoint).
				String("cause", info.Cause.Error()).
				String("cause", meta.Version).
				Message("conn.Conn ban start")
			start := time.Now()
			return func(info trace.DriverConnBanDoneInfo) {
				l.Warn().
					String("endpoint", endpoint).
					Duration("latency", time.Since(start)).
					String("state", info.State.String()).
					String("version", meta.Version).
					Message("conn.Conn ban done")
			}
		}
		t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
			endpoint := info.Endpoint.String()
			l.Info().
				String("endpoint", endpoint).
				String("version", meta.Version).
				Message("conn.Conn allow start")

			start := time.Now()
			return func(info trace.DriverConnAllowDoneInfo) {
				l.Info().
					String("endpoint", endpoint).
					Duration("latency", time.Since(start)).
					String("state", info.State.String()).
					String("version", meta.Version).
					Message("conn.Conn allow done")
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		//nolint:govet
		l := l.WithName(`repeater`)
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			name := info.Name
			event := info.Event
			l.Trace().
				String("name", name).
				String("event", event).
				Message("repeater wake up")
			start := time.Now()
			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				if info.Error == nil {
					l.Trace().
						String("name", name).
						String("event", event).
						Duration("latency", time.Since(start)).
						Message("repeater wake up done")
				} else {
					l.Error().
						String("name", name).
						String("event", event).
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("repeater wake up fail")
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DriverBalancerEvents != 0 {
		//nolint:govet
		l := l.WithName(`balancer`)
		t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			l.Trace().Message(`init start`)
			start := time.Now()
			return func(info trace.DriverBalancerInitDoneInfo) {
				l.Debug().
					Duration("latency", time.Since(start)).
					Message("init done")
			}
		}
		t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
			l.Trace().Message(`close start`)
			start := time.Now()
			return func(info trace.DriverBalancerCloseDoneInfo) {
				if info.Error == nil {
					l.Trace().
						Duration("latency", time.Since(start)).
						Message("close done")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("close failed")
				}
			}
		}
		t.OnBalancerChooseEndpoint = func(
			info trace.DriverBalancerChooseEndpointStartInfo,
		) func(
			trace.DriverBalancerChooseEndpointDoneInfo,
		) {
			l.Trace().Message(`select endpoint start`)
			start := time.Now()
			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error == nil {
					l.Trace().
						Duration("latency", time.Since(start)).
						String("endpoint", info.Endpoint.String()).
						Message("select endpoint done")
				} else {
					l.Warn().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						String("version", meta.Version).
						Message("select endpoint failed")
				}
			}
		}
		t.OnBalancerUpdate = func(
			info trace.DriverBalancerUpdateStartInfo,
		) func(
			trace.DriverBalancerUpdateDoneInfo,
		) {
			l.Trace().
				Bool("needLocalDC", info.NeedLocalDC).
				Message("balancer discovery start")
			start := time.Now()
			return func(info trace.DriverBalancerUpdateDoneInfo) {
				if info.Error == nil {
					l.Info().
						Duration("latency", time.Since(start)).
						Array("endpoints", EndpointInfoSlice(info.Endpoints, l.Array())).
						String("detectedLocalDC", info.LocalDC).
						Message("balancer discovery done")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						Error(info.Error).
						Message("balancer discovery failed")
				}
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		//nolint:govet
		l := l.WithName(`credentials`)
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			l.Trace().Message("get start")
			start := time.Now()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error == nil {
					l.Trace().
						Duration("latency", time.Since(start)).
						String("token", Secret(info.Token)).
						Message("get done")
				} else {
					l.Error().
						Duration("latency", time.Since(start)).
						String("token", Secret(info.Token)).
						Error(info.Error).
						String("version", meta.Version).
						Message("get failed")
				}
			}
		}
	}
	return t
}
