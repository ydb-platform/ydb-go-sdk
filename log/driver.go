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
			info trace.ResolveStartInfo,
		) func(
			trace.ResolveDoneInfo,
		) {
			target := info.Target
			addresses := info.Resolved
			log.Tracef(`update start {target:"%s",resolved:%v}`,
				target,
				addresses,
			)
			return func(info trace.ResolveDoneInfo) {
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
		t.OnNetRead = func(info trace.NetReadStartInfo) func(trace.NetReadDoneInfo) {
			address := info.Address
			log.Tracef(`read start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info trace.NetReadDoneInfo) {
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
		t.OnNetWrite = func(info trace.NetWriteStartInfo) func(trace.NetWriteDoneInfo) {
			address := info.Address
			log.Tracef(`write start {address:"%s"}`, address)
			start := time.Now()
			return func(info trace.NetWriteDoneInfo) {
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
		t.OnNetDial = func(info trace.NetDialStartInfo) func(trace.NetDialDoneInfo) {
			address := info.Address
			log.Debugf(`dial start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info trace.NetDialDoneInfo) {
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
		t.OnNetClose = func(info trace.NetCloseStartInfo) func(trace.NetCloseDoneInfo) {
			address := info.Address
			log.Debugf(`close start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info trace.NetCloseDoneInfo) {
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
	// nolint:nestif
	if details&trace.DriverCoreEvents != 0 {
		// nolint:govet
		log := log.WithName(`core`)
		t.OnInit = func(info trace.InitStartInfo) func(trace.InitDoneInfo) {
			endpoint := info.Endpoint
			database := info.Database
			secure := info.Secure
			log.Infof(
				`init start {version:%s,endpoint:"%s",database:"%s",secure:%v}`,
				meta.Version,
				endpoint,
				database,
				secure,
			)
			start := time.Now()
			return func(info trace.InitDoneInfo) {
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
		t.OnClose = func(info trace.CloseStartInfo) func(trace.CloseDoneInfo) {
			log.Infof(`close start`)
			start := time.Now()
			return func(info trace.CloseDoneInfo) {
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
		t.OnConnTake = func(info trace.ConnTakeStartInfo) func(trace.ConnTakeDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Tracef(`take start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.ConnTakeDoneInfo) {
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
		t.OnConnRelease = func(info trace.ConnReleaseStartInfo) func(trace.ConnReleaseDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Tracef(`release start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.ConnReleaseDoneInfo) {
				log.Tracef(`release done {endpoint:%v,latency:"%v",locks:%d}`,
					endpoint,
					time.Since(start),
					info.Lock,
				)
			}
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Tracef(`conn state change start {endpoint:%v,state:"%s"}`,
				endpoint,
				info.State,
			)
			start := time.Now()
			return func(info trace.ConnStateChangeDoneInfo) {
				log.Tracef(`conn state change done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			endpoint := info.Endpoint.String()
			method := string(info.Method)
			log.Tracef(`invoke start {endpoint:%v,method:"%s"}`,
				endpoint,
				method,
			)
			start := time.Now()
			return func(info trace.ConnInvokeDoneInfo) {
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
			info trace.ConnNewStreamStartInfo,
		) func(
			trace.ConnNewStreamRecvInfo,
		) func(
			trace.ConnNewStreamDoneInfo,
		) {
			endpoint := info.Endpoint.String()
			method := string(info.Method)
			log.Tracef(`streaming start {endpoint:%v,method:"%s"}`,
				endpoint,
				method,
			)
			start := time.Now()
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
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
				return func(info trace.ConnNewStreamDoneInfo) {
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
	if details&trace.DriverClusterEvents != 0 {
		// nolint:govet
		log := log.WithName(`cluster`)
		t.OnClusterInit = func(info trace.ClusterInitStartInfo) func(trace.ClusterInitDoneInfo) {
			log.Tracef(`init start`)
			start := time.Now()
			return func(info trace.ClusterInitDoneInfo) {
				log.Debugf(`init done {latency:"%v"}`,
					time.Since(start),
				)
			}
		}
		t.OnClusterClose = func(info trace.ClusterCloseStartInfo) func(trace.ClusterCloseDoneInfo) {
			log.Tracef(`close start`)
			start := time.Now()
			return func(info trace.ClusterCloseDoneInfo) {
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
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			log.Tracef(`get start`)
			start := time.Now()
			return func(info trace.ClusterGetDoneInfo) {
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
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Debugf(`insert start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.ClusterInsertDoneInfo) {
				log.Infof(`insert done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Debugf(`remove start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.ClusterRemoveDoneInfo) {
				log.Infof(`remove done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Debugf(`update start {endpoint:%v}`,
				endpoint,
			)
			start := time.Now()
			return func(info trace.ClusterUpdateDoneInfo) {
				log.Infof(`update done {endpoint:%v,latency:"%v",state:"%s"}`,
					endpoint,
					time.Since(start),
					info.State,
				)
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			endpoint := info.Endpoint.String()
			log.Warnf(`pessimize start {endpoint:%v,cause:"%s"}`,
				endpoint,
				info.Cause,
			)
			start := time.Now()
			return func(info trace.PessimizeNodeDoneInfo) {
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
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			log.Tracef(`get start`)
			start := time.Now()
			return func(info trace.GetCredentialsDoneInfo) {
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
