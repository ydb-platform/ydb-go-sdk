package log

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
			l.Tracef(`update start {target:"%s",resolved:%v}`,
				target,
				addresses,
			)
			return func(info trace.DriverResolveDoneInfo) {
				if info.Error == nil {
					l.Infof(`update done {target:"%s",resolved:%v}`,
						target,
						addresses,
					)
				} else {
					l.Warnf(`update failed {target:"%s",resolved:%v,error:"%v",version:"%s"}`,
						target,
						addresses,
						info.Error,
						meta.Version,
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
			l.Infof(
				`init start {version:%s,endpoint:"%s",database:"%s",secure:%v}`,
				meta.VersionMajor+"."+meta.VersionMinor+"."+meta.VersionPatch,
				endpoint,
				database,
				secure,
			)
			start := time.Now()
			return func(info trace.DriverInitDoneInfo) {
				if info.Error == nil {
					l.Infof(
						`init done {endpoint:"%s",database:"%s",secure:%t,latency:"%v"}`,
						endpoint,
						database,
						secure,
						time.Since(start),
					)
				} else {
					l.Warnf(`init failed {endpoint:"%s",database:"%s",secure:%t,latency:"%v",error:"%s",version:"%s"}`,
						endpoint,
						database,
						secure,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnClose = func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			l.Infof(`close start`)
			start := time.Now()
			return func(info trace.DriverCloseDoneInfo) {
				if info.Error == nil {
					l.Infof(
						`close done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Warnf(`close failed {latency:"%v",error:"%s",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
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
			l.Tracef(`take start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverConnTakeDoneInfo) {
				if info.Error == nil {
					l.Tracef(`take done {endpoint:%v,latency:"%v"}`,
						endpoint,
						time.Since(start),
					)
				} else {
					l.Warnf(`take failed {endpoint:%v,latency:"%v",error:"%s",version:"%s"}`,
						endpoint,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			endpoint := info.Endpoint.String()
			l.Tracef(`conn state change start {endpoint:%v,state:"%s"}`,
				endpoint,
				info.State,
			)
			start := time.Now()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				l.Tracef(`conn state change done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			endpoint := info.Endpoint
			l.Tracef(`conn park start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error == nil {
					l.Tracef(`conn park done {endpoint:%v,latency:"%v"}`,
						endpoint,
						time.Since(start),
					)
				} else {
					l.Warnf(`conn park fail {endpoint:%v,latency:"%v",error:"%s",version:"%s"}`,
						endpoint,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			endpoint := info.Endpoint
			l.Tracef(`conn close start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					l.Tracef(`conn close done {endpoint:%v,latency:"%v"}`,
						endpoint,
						time.Since(start),
					)
				} else {
					l.Warnf(`conn close fail {endpoint:%v,latency:"%v",error:"%s",version:"%s"}`,
						endpoint,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			endpoint := info.Endpoint.String()
			method := string(info.Method)
			l.Tracef(`invoke start {endpoint:%v,method:"%s"}`,
				endpoint,
				method,
			)
			start := time.Now()
			return func(info trace.DriverConnInvokeDoneInfo) {
				if info.Error == nil {
					l.Tracef(`invoke done {endpoint:%v,method:"%s",latency:"%v",,metadata:%v}`,
						endpoint,
						method,
						time.Since(start),
						info.Metadata,
					)
				} else {
					l.Warnf(`invoke failed {endpoint:%v,method:"%s",latency:"%v",error:"%s",,metadata:%v,version:"%s"}`,
						endpoint,
						method,
						time.Since(start),
						info.Error,
						info.Metadata,
						meta.Version,
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
			endpoint := info.Endpoint.String()
			method := string(info.Method)
			l.Tracef(`streaming start {endpoint:%v,method:"%s"}`,
				endpoint,
				method,
			)
			start := time.Now()
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				if info.Error == nil {
					l.Tracef(`streaming intermediate receive {endpoint:%v,method:"%s",latency:"%v"}`,
						endpoint,
						method,
						time.Since(start),
					)
				} else {
					l.Warnf(`streaming intermediate fail {endpoint:%v,method:"%s",latency:"%v",error:"%s",version:"%s"}`,
						endpoint,
						method,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
				return func(info trace.DriverConnNewStreamDoneInfo) {
					if info.Error == nil {
						l.Tracef(`streaming done {endpoint:%v,method:"%s",latency:"%v",metadata:%v}`,
							endpoint,
							method,
							time.Since(start),
							info.Metadata,
						)
					} else {
						l.Warnf(`streaming done {endpoint:%v,method:"%s",latency:"%v",error:"%s",,metadata:%v,version:"%s"}`,
							endpoint,
							method,
							time.Since(start),
							info.Error,
							info.Metadata,
							meta.Version,
						)
					}
				}
			}
		}
		t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			endpoint := info.Endpoint.String()
			l.Warnf(`conn.Conn ban start {endpoint:%v,cause:"%s",version:"%s"}`,
				endpoint,
				info.Cause,
				meta.Version,
			)
			start := time.Now()
			return func(info trace.DriverConnBanDoneInfo) {
				l.Warnf(`conn.Conn ban done {endpoint:%v,latency:"%v",state:"%s",version:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
					meta.Version,
				)
			}
		}
		t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
			endpoint := info.Endpoint.String()
			l.Infof(`conn.Conn allow start {endpoint:%v,version:"%s"}`,
				endpoint,
				meta.Version,
			)

			start := time.Now()
			return func(info trace.DriverConnAllowDoneInfo) {
				l.Infof(`conn.Conn allow done {endpoint:%v,latency:"%v",state:"%s",version:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
					meta.Version,
				)
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		//nolint:govet
		l := l.WithName(`repeater`)
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			name := info.Name
			event := info.Event
			l.Tracef(`repeater wake up {name:"%s",event:"%s"}`,
				name,
				event,
			)
			start := time.Now()
			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				if info.Error == nil {
					l.Tracef(`repeater wake up done {name:"%s",event:"%s",latency:"%v"}`,
						name,
						event,
						time.Since(start),
					)
				} else {
					l.Errorf(`repeater wake up fail {name:"%s",event:"%s",latency:"%v",error:"%v",version:"%s"}`,
						name,
						event,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.DriverBalancerEvents != 0 {
		//nolint:govet
		l := l.WithName(`balancer`)
		t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			l.Tracef(`init start`)
			start := time.Now()
			return func(info trace.DriverBalancerInitDoneInfo) {
				l.Debugf(`init done {latency:"%v"}`,
					time.Since(start),
				)
			}
		}
		t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
			l.Tracef(`close start`)
			start := time.Now()
			return func(info trace.DriverBalancerCloseDoneInfo) {
				if info.Error == nil {
					l.Tracef(`close done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`close failed {latency:"%v",error:"%s",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnBalancerChooseEndpoint = func(
			info trace.DriverBalancerChooseEndpointStartInfo,
		) func(
			trace.DriverBalancerChooseEndpointDoneInfo,
		) {
			l.Tracef(`select endpoint start`)
			start := time.Now()
			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error == nil {
					l.Tracef(`select endpoint done {latency:"%v",endpoint:%v}`,
						time.Since(start),
						info.Endpoint.String(),
					)
				} else {
					l.Warnf(`select endpoint failed {latency:"%v",error:"%s",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnBalancerClusterDiscoveryAttempt = func(
			info trace.DriverBalancerClusterDiscoveryAttemptStartInfo,
		) func(
			trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
		) {
			l.Tracef(`trying to cluster discovery {address:"%s"}`, info.Address)
			start := time.Now()
			return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
				if info.Error == nil {
					l.Tracef(`cluster discovery done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					l.Errorf(`cluster discovery failed {latency:"%v",error:"%s",version:"%s"}`,
						time.Since(start),
						info.Error,
						meta.Version,
					)
				}
			}
		}
		t.OnBalancerUpdate = func(
			info trace.DriverBalancerUpdateStartInfo,
		) func(
			trace.DriverBalancerUpdateDoneInfo,
		) {
			l.Tracef(
				`balancer update start {needLocalDC: "%v"}`,
				info.NeedLocalDC,
			)
			start := time.Now()
			return func(info trace.DriverBalancerUpdateDoneInfo) {
				l.Infof(
					`balancer update done {latency:"%v", endpoints: "%v", detectedLocalDC: "%v"}`,
					time.Since(start),
					info.Endpoints,
					info.LocalDC,
				)
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		//nolint:govet
		l := l.WithName(`credentials`)
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			l.Tracef(`get start`)
			start := time.Now()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error == nil {
					l.Tracef(`get done {latency:"%v",token:"%s"}`,
						time.Since(start),
						Secret(info.Token),
					)
				} else {
					l.Errorf(`get failed {latency:"%v",token:"%s",error:"%s",version:"%s"}`,
						time.Since(start),
						Secret(info.Token),
						info.Error,
						meta.Version,
					)
				}
			}
		}
	}
	return t
}
