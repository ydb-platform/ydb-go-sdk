// nolint:revive
package ydb_log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes ydb_trace.Driver with internal logging
func Driver(log Logger, details ydb_trace.Details) ydb_trace.Driver {
	log = log.WithName(`driver`)
	t := ydb_trace.Driver{}
	// nolint:nestif
	if details&ydb_trace.DriverNetEvents != 0 {
		// nolint:govet
		log := log.WithName(`net`)
		t.OnNetRead = func(info ydb_trace.NetReadStartInfo) func(ydb_trace.NetReadDoneInfo) {
			address := info.Address
			log.Tracef(`read start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info ydb_trace.NetReadDoneInfo) {
				if info.Error == nil {
					log.Tracef(`read done {latency:"%s",address:"%s",received:%d}`,
						time.Since(start),
						address,
						info.Received,
					)
				} else {
					log.Warnf(`read failed {latency:"%s",address:"%s",received:%d,error:"%s"}`,
						time.Since(start),
						address,
						info.Received,
						info.Error,
					)
				}
			}
		}
		t.OnNetWrite = func(info ydb_trace.NetWriteStartInfo) func(ydb_trace.NetWriteDoneInfo) {
			address := info.Address
			log.Tracef(`write start {address:"%s"}`, address)
			start := time.Now()
			return func(info ydb_trace.NetWriteDoneInfo) {
				if info.Error == nil {
					log.Tracef(`write done {latency:"%s",address:"%s",sent:%d}`,
						time.Since(start),
						address,
						info.Sent,
					)
				} else {
					log.Warnf(`write failed {latency:"%s",address:"%s",sent:%d,error:"%s"}`,
						time.Since(start),
						address,
						info.Sent,
						info.Error,
					)
				}
			}
		}
		t.OnNetDial = func(info ydb_trace.NetDialStartInfo) func(ydb_trace.NetDialDoneInfo) {
			address := info.Address
			log.Debugf(`dial start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info ydb_trace.NetDialDoneInfo) {
				if info.Error == nil {
					log.Debugf(`dial done {latency:"%s",address:"%s"}`,
						time.Since(start),
						address,
					)
				} else {
					log.Errorf(`dial failed {latency:"%s",address:"%s",error:"%s"}`,
						time.Since(start),
						address,
						info.Error,
					)
				}
			}
		}
		t.OnNetClose = func(info ydb_trace.NetCloseStartInfo) func(ydb_trace.NetCloseDoneInfo) {
			address := info.Address
			log.Debugf(`close start {address:"%s"}`,
				address,
			)
			start := time.Now()
			return func(info ydb_trace.NetCloseDoneInfo) {
				if info.Error == nil {
					log.Debugf(`close done {latency:"%s",address:"%s"}`,
						time.Since(start),
						address,
					)
				} else {
					log.Warnf(`close failed {latency:"%s",address:"%s",error:"%s"}`,
						time.Since(start),
						address,
						info.Error,
					)
				}
			}
		}
	}
	// nolint:nestif
	if details&ydb_trace.DriverCoreEvents != 0 {
		// nolint:govet
		log := log.WithName(`core`)
		t.OnInit = func(info ydb_trace.InitStartInfo) func(ydb_trace.InitDoneInfo) {
			endpoint := info.Endpoint
			database := info.Database
			secure := info.Secure
			log.Infof(
				`init start {endpoint:"%s",database:"%s",endpoint:%v}`,
				endpoint,
				database,
				secure,
			)
			start := time.Now()
			return func(info ydb_trace.InitDoneInfo) {
				if info.Error == nil {
					log.Infof(
						`init done {{endpoint:"%s",database:"%s",endpoint:%v,latency:"%s"}`,
						endpoint,
						database,
						secure,
						time.Since(start),
					)
				} else {
					log.Warnf(
						`init failed {{endpoint:"%s",database:"%s",endpoint:%v,latency:"%s",error:"%s"}`,
						endpoint,
						database,
						secure,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnClose = func(info ydb_trace.CloseStartInfo) func(ydb_trace.CloseDoneInfo) {
			log.Infof(`close start`)
			start := time.Now()
			return func(info ydb_trace.CloseDoneInfo) {
				if info.Error == nil {
					log.Infof(
						`close done {latency:"%s"}`,
						time.Since(start),
					)
				} else {
					log.Warnf(
						`close failed {latency:"%s",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnConnTake = func(info ydb_trace.ConnTakeStartInfo) func(ydb_trace.ConnTakeDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			log.Tracef(`take start {address:"%s",local:%t}`,
				address,
				local,
			)
			start := time.Now()
			return func(info ydb_trace.ConnTakeDoneInfo) {
				if info.Error == nil {
					log.Tracef(`take done {latency:"%s",address:"%s",local:%t}`,
						time.Since(start),
						address,
						local,
					)
				} else {
					log.Warnf(`take failed {latency:"%s",address:"%s",local:%t,error:"%s"}`,
						time.Since(start),
						address,
						local,
						info.Error,
					)
				}
			}
		}
		t.OnConnRelease = func(info ydb_trace.ConnReleaseStartInfo) func(ydb_trace.ConnReleaseDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			log.Tracef(`release start {address:"%s",local:%t}`,
				address,
				local,
			)
			start := time.Now()
			return func(info ydb_trace.ConnReleaseDoneInfo) {
				log.Tracef(`release done {latency:"%s",address:"%s",local:%t,locks:%d}`,
					time.Since(start),
					address,
					local,
					info.Lock,
				)
			}
		}
		t.OnConnStateChange = func(info ydb_trace.ConnStateChangeStartInfo) func(ydb_trace.ConnStateChangeDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			log.Tracef(`conn state change start {address:"%s",local:%t,state:"%s"}`,
				address,
				local,
				info.State,
			)
			start := time.Now()
			return func(info ydb_trace.ConnStateChangeDoneInfo) {
				log.Tracef(`conn state change done {latency:"%s",address:"%s",local:%t,state:"%s"}`,
					time.Since(start),
					address,
					local,
					info.State,
				)
			}
		}
		t.OnConnInvoke = func(info ydb_trace.ConnInvokeStartInfo) func(ydb_trace.ConnInvokeDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			method := string(info.Method)
			log.Tracef(`invoke start {address:"%s",local:%t,method:"%s"}`,
				address,
				local,
				method,
			)
			start := time.Now()
			return func(info ydb_trace.ConnInvokeDoneInfo) {
				if info.Error == nil {
					log.Tracef(`invoke done {latency:"%s",address:"%s",local:%t,method:"%s"}`,
						time.Since(start),
						address,
						local,
						method,
					)
				} else {
					log.Warnf(`invoke failed {latency:"%s",address:"%s",local:%t,method:"%s",error:"%s"}`,
						time.Since(start),
						address,
						local,
						method,
						info.Error,
					)
				}
			}
		}
		t.OnConnNewStream = func(
			info ydb_trace.ConnNewStreamStartInfo,
		) func(
			ydb_trace.ConnNewStreamRecvInfo,
		) func(
			ydb_trace.ConnNewStreamDoneInfo,
		) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			method := string(info.Method)
			log.Tracef(`streaming start {address:"%s",local:%t,method:"%s"}`,
				address,
				local,
				method,
			)
			start := time.Now()
			return func(info ydb_trace.ConnNewStreamRecvInfo) func(ydb_trace.ConnNewStreamDoneInfo) {
				if info.Error == nil {
					log.Tracef(`streaming intermediate receive {latency:"%s",address:"%s",local:%t,method:"%s"}`,
						time.Since(start),
						address,
						local,
						method,
					)
				} else {
					log.Warnf(`streaming intermediate receive failed {latency:"%s",address:"%s",local:%t,method:"%s",error:"%s"}`,
						time.Since(start),
						address,
						local,
						method,
						info.Error,
					)
				}
				return func(info ydb_trace.ConnNewStreamDoneInfo) {
					if info.Error == nil {
						log.Tracef(`streaming done {latency:"%s",address:"%s",local:%t,method:"%s"}`,
							time.Since(start),
							address,
							local,
							method,
						)
					} else {
						log.Warnf(`streaming failed {latency:"%s",address:"%s",local:%t,method:"%s",error:"%s"}`,
							time.Since(start),
							address,
							local,
							method,
							info.Error,
						)
					}
				}
			}
		}
	}
	if details&ydb_trace.DriverDiscoveryEvents != 0 {
		// nolint:govet
		log := log.WithName(`discovery`)
		t.OnDiscovery = func(info ydb_trace.DiscoveryStartInfo) func(ydb_trace.DiscoveryDoneInfo) {
			log.Debugf(`discover start`)
			start := time.Now()
			return func(info ydb_trace.DiscoveryDoneInfo) {
				if info.Error == nil {
					log.Debugf(`discover done {latency:"%s",endpoints:%v}`,
						time.Since(start),
						info.Endpoints,
					)
				} else {
					log.Errorf(`discover failed {latency:"%s",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
	}
	if details&ydb_trace.DriverClusterEvents != 0 {
		// nolint:govet
		log := log.WithName(`cluster`)
		t.OnClusterGet = func(info ydb_trace.ClusterGetStartInfo) func(ydb_trace.ClusterGetDoneInfo) {
			log.Tracef(`get start`)
			start := time.Now()
			return func(info ydb_trace.ClusterGetDoneInfo) {
				if info.Error == nil {
					log.Tracef(`get done {latency:"%s",address:"%s",local:%t}`,
						time.Since(start),
						info.Endpoint.Address(),
						info.Endpoint.LocalDC(),
					)
				} else {
					log.Warnf(`get failed {latency:"%s",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnClusterInsert = func(info ydb_trace.ClusterInsertStartInfo) func(ydb_trace.ClusterInsertDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			log.Debugf(`insert start {address:"%s",local:%t}`,
				address,
				local,
			)
			start := time.Now()
			return func(info ydb_trace.ClusterInsertDoneInfo) {
				log.Infof(`insert done {latency:"%s",address:"%s",local:%t,state:"%s"}`,
					time.Since(start),
					address,
					local,
					info.State,
				)
			}
		}
		t.OnClusterRemove = func(info ydb_trace.ClusterRemoveStartInfo) func(ydb_trace.ClusterRemoveDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			log.Debugf(`remove start {address:"%s",local:%t}`,
				address,
				local,
			)
			start := time.Now()
			return func(info ydb_trace.ClusterRemoveDoneInfo) {
				log.Infof(`remove done {latency:"%s",address:"%s",local:%t,state:"%s"}`,
					time.Since(start),
					address,
					local,
					info.State,
				)
			}
		}
		t.OnClusterUpdate = func(info ydb_trace.ClusterUpdateStartInfo) func(ydb_trace.ClusterUpdateDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			log.Debugf(`update start {address:"%s",local:%t}`,
				address,
				local,
			)
			start := time.Now()
			return func(info ydb_trace.ClusterUpdateDoneInfo) {
				log.Infof(`update done {latency:"%s",address:"%s",local:%t,state:"%s"}`,
					time.Since(start),
					address,
					local,
					info.State,
				)
			}
		}
		t.OnPessimizeNode = func(info ydb_trace.PessimizeNodeStartInfo) func(ydb_trace.PessimizeNodeDoneInfo) {
			address := info.Endpoint.Address()
			local := info.Endpoint.LocalDC()
			log.Warnf(`pessimize start {address:"%s",local:%t,cause:'"%s"'}`,
				address,
				local,
				info.Cause,
			)
			start := time.Now()
			return func(info ydb_trace.PessimizeNodeDoneInfo) {
				log.Warnf(`pessimize done {latency:"%s",address:"%s",local:%t,state:"%s"}`,
					time.Since(start),
					address,
					local,
					info.State,
				)
			}
		}
	}
	if details&ydb_trace.DriverCredentialsEvents != 0 {
		// nolint:govet
		log := log.WithName(`credentials`)
		t.OnGetCredentials = func(info ydb_trace.GetCredentialsStartInfo) func(ydb_trace.GetCredentialsDoneInfo) {
			log.Tracef(`get start`)
			start := time.Now()
			return func(info ydb_trace.GetCredentialsDoneInfo) {
				if info.Error == nil {
					log.Tracef(`get done {latency:"%s",ok:%t}`,
						time.Since(start),
						info.TokenOk,
					)
				} else {
					log.Errorf(`get failed {latency:"%s",ok:%t,error:"%s"}`,
						time.Since(start),
						info.TokenOk,
						info.Error,
					)
				}
			}
		}
	}
	return t
}
