package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with internal logging
func Driver(log Logger, details trace.Details) (t trace.Driver) {
	log = log.WithName(`driver`)
	if details&trace.DriverResolverEvents != 0 {
		// nolint:govet
		log := log.WithName(`resolver`)
		t.OnResolve = func(
			info trace.DriverResolveStartInfo,
		) func(
			trace.DriverResolveDoneInfo,
		) {
			target := info.Target
			addresses := info.Resolved
			log.Tracef(`update start {target:"%s",resolved:%v}`,
				target,
				addresses,
			)
			return func(info trace.DriverResolveDoneInfo) {
				if info.Error == nil {
					log.Infof(`update done {target:"%s",resolved:%v}`,
						target,
						addresses,
					)
				} else {
					log.Warnf(`update failed {target:"%s",resolved:%v,error:"%v"}`,
						target,
						addresses,
						info.Error,
					)
				}
			}
		}
	}
	// nolint:nestif
	if details&trace.DriverNetEvents != 0 {
		// nolint:govet
		log := log.WithName(`net`)
		t.OnNetRead = func(info trace.DriverNetReadStartInfo) func(trace.DriverNetReadDoneInfo) {
			address := info.Address
			log.Tracef(`read start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info trace.DriverNetReadDoneInfo) {
				if info.Error == nil {
					log.Tracef(`read done {latency:"%v",address:"%s",received:%d}`,
						time.Since(start),
						address,
						info.Received,
					)
				} else {
					log.Warnf(`read failed {latency:"%v",address:"%s",received:%d,error:"%s"}`,
						time.Since(start),
						address,
						info.Received,
						info.Error,
					)
				}
			}
		}
		t.OnNetWrite = func(info trace.DriverNetWriteStartInfo) func(trace.DriverNetWriteDoneInfo) {
			address := info.Address
			log.Tracef(`write start {address:"%s"}`, address)
			start := time.Now()
			return func(info trace.DriverNetWriteDoneInfo) {
				if info.Error == nil {
					log.Tracef(`write done {latency:"%v",address:"%s",sent:%d}`,
						time.Since(start),
						address,
						info.Sent,
					)
				} else {
					log.Warnf(`write failed {latency:"%v",address:"%s",sent:%d,error:"%s"}`,
						time.Since(start),
						address,
						info.Sent,
						info.Error,
					)
				}
			}
		}
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			address := info.Address
			log.Debugf(`dial start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info trace.DriverNetDialDoneInfo) {
				if info.Error == nil {
					log.Debugf(`dial done {latency:"%v",address:"%s"}`,
						time.Since(start),
						address,
					)
				} else {
					log.Errorf(`dial failed {latency:"%v",address:"%s",error:"%s"}`,
						time.Since(start),
						address,
						info.Error,
					)
				}
			}
		}
		t.OnNetClose = func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			address := info.Address
			log.Debugf(`close start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info trace.DriverNetCloseDoneInfo) {
				if info.Error == nil {
					log.Debugf(`close done {latency:"%v",address:"%s"}`,
						time.Since(start),
						address,
					)
				} else {
					log.Warnf(`close failed {latency:"%v",address:"%s",error:"%s"}`,
						time.Since(start),
						address,
						info.Error,
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
			log.Infof(
				`init start {version:%s,endpoint:"%s",database:"%s",secure:%v}`,
				meta.VersionMajor+"."+meta.VersionMinor+"."+meta.VersionPatch,
				endpoint,
				database,
				secure,
			)
			start := time.Now()
			return func(info trace.DriverInitDoneInfo) {
				if info.Error == nil {
					log.Infof(
						`init done {endpoint:"%s",database:"%s",secure:%t,latency:"%v"}`,
						endpoint,
						database,
						secure,
						time.Since(start),
					)
				} else {
					log.Warnf(
						`init failed {endpoint:"%s",database:"%s",secure:%t,latency:"%v",error:"%s"}`,
						endpoint,
						database,
						secure,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnClose = func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			log.Infof(`close start`)
			start := time.Now()
			return func(info trace.DriverCloseDoneInfo) {
				if info.Error == nil {
					log.Infof(
						`close done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					log.Warnf(
						`close failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
	}
	// nolint:nestif
	if details&trace.DriverConnEvents != 0 {
		// nolint:govet
		log := log.WithName(`conn`)
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Tracef(`take start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverConnTakeDoneInfo) {
				if info.Error == nil {
					log.Tracef(`take done {endpoint:%v,latency:"%v"}`,
						endpoint,
						time.Since(start),
					)
				} else {
					log.Warnf(`take failed {endpoint:%v,latency:"%v",error:"%s"}`,
						endpoint,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnConnUsagesChange = func(info trace.DriverConnUsagesChangeInfo) {
			log.Tracef(`change conn usages {endpoint:%v,usages:%d}`,
				info.Endpoint.String(),
				info.Usages,
			)
		}
		t.OnConnStreamUsagesChange = func(info trace.DriverConnStreamUsagesChangeInfo) {
			log.Tracef(`change conn stream usages {endpoint:%v,usages:%d}`,
				info.Endpoint.String(),
				info.Usages,
			)
		}
		t.OnConnRelease = func(info trace.DriverConnReleaseStartInfo) func(trace.DriverConnReleaseDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Tracef(`release conn {endpoint:%v}`,
				endpoint,
			)
			return func(info trace.DriverConnReleaseDoneInfo) {
				if info.Error == nil {
					log.Tracef(`release conn done {endpoint:%v}`,
						endpoint,
					)
				} else {
					log.Warnf(`release conn failed {endpoint:%v,error:"%s"}`,
						endpoint,
						info.Error,
					)
				}
			}
		}
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Tracef(`conn state change start {endpoint:%v,state:"%s"}`,
				endpoint,
				info.State,
			)
			start := time.Now()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				log.Tracef(`conn state change done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			endpoint := info.Endpoint
			log.Tracef(`conn park start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error == nil {
					log.Tracef(`conn park done {endpoint:%v,latency:"%v"}`,
						endpoint,
						time.Since(start),
					)
				} else {
					log.Warnf(`conn park fail {endpoint:%v,latency:"%v",error:"%s"}`,
						endpoint,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			endpoint := info.Endpoint
			log.Tracef(`conn close start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					log.Tracef(`conn close done {endpoint:%v,latency:"%v"}`,
						endpoint,
						time.Since(start),
					)
				} else {
					log.Warnf(`conn close fail {endpoint:%v,latency:"%v",error:"%s"}`,
						endpoint,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			endpoint := info.Endpoint.String()
			method := string(info.Method)
			log.Tracef(`invoke start {endpoint:%v,method:"%s"}`,
				endpoint,
				method,
			)
			start := time.Now()
			return func(info trace.DriverConnInvokeDoneInfo) {
				if info.Error == nil {
					log.Tracef(`invoke done {endpoint:%v,method:"%s",latency:"%v"}`,
						endpoint,
						method,
						time.Since(start),
					)
				} else {
					log.Warnf(`invoke failed {endpoint:%v,method:"%s",latency:"%v",error:"%s"}`,
						endpoint,
						method,
						time.Since(start),
						info.Error,
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
			log.Tracef(`streaming start {endpoint:%v,method:"%s"}`,
				endpoint,
				method,
			)
			start := time.Now()
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				if info.Error == nil {
					log.Tracef(`streaming intermediate receive {endpoint:%v,method:"%s",latency:"%v"}`,
						endpoint,
						method,
						time.Since(start),
					)
				} else {
					log.Warnf(`streaming intermediate fail {endpoint:%v,method:"%s",latency:"%v",error:"%s"}`,
						endpoint,
						method,
						time.Since(start),
						info.Error,
					)
				}
				return func(info trace.DriverConnNewStreamDoneInfo) {
					if info.Error == nil {
						log.Tracef(`streaming done {endpoint:%v,method:"%s",latency:"%v"}`,
							endpoint,
							method,
							time.Since(start),
						)
					} else {
						log.Warnf(`streaming done {endpoint:%v,method:"%s",latency:"%v",error:"%s"}`,
							endpoint,
							method,
							time.Since(start),
							info.Error,
						)
					}
				}
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		// nolint:govet
		log := log.WithName(`repeater`)
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterTickStartInfo) func(trace.DriverRepeaterTickDoneInfo) {
			name := info.Name
			event := info.Event
			log.Tracef(`repeater wake up {name:"%s",event:"%s"}`,
				name,
				event,
			)
			start := time.Now()
			return func(info trace.DriverRepeaterTickDoneInfo) {
				if info.Error == nil {
					log.Tracef(`repeater wake up done {name:"%s",event:"%s",latency:"%v"}`,
						name,
						event,
						time.Since(start),
					)
				} else {
					log.Errorf(`repeater wake up fail {name:"%s",event:"%s",latency:"%v",error:"%v"}`,
						name,
						event,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
	}
	if details&trace.DriverClusterEvents != 0 {
		// nolint:govet
		log := log.WithName(`cluster`)
		t.OnClusterInit = func(info trace.DriverClusterInitStartInfo) func(trace.DriverClusterInitDoneInfo) {
			log.Tracef(`init start`)
			start := time.Now()
			return func(info trace.DriverClusterInitDoneInfo) {
				log.Debugf(`init done {latency:"%v"}`,
					time.Since(start),
				)
			}
		}
		t.OnClusterClose = func(info trace.DriverClusterCloseStartInfo) func(trace.DriverClusterCloseDoneInfo) {
			log.Tracef(`close start`)
			start := time.Now()
			return func(info trace.DriverClusterCloseDoneInfo) {
				if info.Error == nil {
					log.Tracef(`close done {latency:"%v"}`,
						time.Since(start),
					)
				} else {
					log.Errorf(`close failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnClusterGet = func(info trace.DriverClusterGetStartInfo) func(trace.DriverClusterGetDoneInfo) {
			log.Tracef(`get start`)
			start := time.Now()
			return func(info trace.DriverClusterGetDoneInfo) {
				if info.Error == nil {
					log.Tracef(`get done {latency:"%v",endpoint:%v}`,
						time.Since(start),
						info.Endpoint.String(),
					)
				} else {
					log.Warnf(`get failed {latency:"%v",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnClusterInsert = func(info trace.DriverClusterInsertStartInfo) func(trace.DriverClusterInsertDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Debugf(`insert start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverClusterInsertDoneInfo) {
				log.Infof(`insert done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnClusterRemove = func(info trace.DriverClusterRemoveStartInfo) func(trace.DriverClusterRemoveDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Debugf(`remove start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.DriverClusterRemoveDoneInfo) {
				log.Infof(`remove done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnPessimizeNode = func(info trace.DriverPessimizeNodeStartInfo) func(trace.DriverPessimizeNodeDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Warnf(`pessimize start {endpoint:%v,cause:"%s"}`,
				endpoint,
				info.Cause,
			)
			start := time.Now()
			return func(info trace.DriverPessimizeNodeDoneInfo) {
				log.Warnf(`pessimize done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		// nolint:govet
		log := log.WithName(`credentials`)
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			log.Tracef(`get start`)
			start := time.Now()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error == nil {
					log.Tracef(`get done {latency:"%v",token:"%s"}`,
						time.Since(start),
						Secret(info.Token),
					)
				} else {
					log.Errorf(`get failed {latency:"%v",token:"%s",error:"%s"}`,
						time.Since(start),
						Secret(info.Token),
						info.Error,
					)
				}
			}
		}
	}
	return t
}
