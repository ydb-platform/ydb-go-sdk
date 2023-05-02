package log

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with logging events from details
func Driver(l Logger, d trace.Detailer, opts ...Option) (t trace.Driver) {
	return internalDriver(wrapLogger(l, opts...), d)
}

func internalDriver(l *wrapper, d trace.Detailer) (t trace.Driver) { //nolint:gocyclo
	t.OnResolve = func(
		info trace.DriverResolveStartInfo,
	) func(
		trace.DriverResolveDoneInfo,
	) {
		if d.Details()&trace.DriverResolverEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "driver", "resolver", "update")
		target := info.Target
		addresses := info.Resolved
		l.Log(ctx, "start",
			String("target", target),
			Strings("resolved", addresses),
		)
		return func(info trace.DriverResolveDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "done",
					String("target", target),
					Strings("resolved", addresses),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					String("target", target),
					Strings("resolved", addresses),
					version(),
				)
			}
		}
	}
	t.OnInit = func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
		if d.Details()&trace.DriverEvents == 0 {
			return nil
		}
		endpoint := info.Endpoint
		database := info.Database
		secure := info.Secure
		ctx := with(*info.Context, TRACE, "ydb", "driver", "resolver", "init")
		l.Log(ctx, "start",
			String("endpoint", endpoint),
			String("database", database),
			Bool("secure", secure),
		)
		start := time.Now()
		return func(info trace.DriverInitDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "done",
					String("endpoint", endpoint),
					String("database", database),
					Bool("secure", secure),
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					String("endpoint", endpoint),
					String("database", database),
					Bool("secure", secure),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnClose = func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
		if d.Details()&trace.DriverEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "resolver", "close")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.DriverCloseDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "done",
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnConnDial = func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "dial")
		endpoint := info.Endpoint
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
		)
		start := time.Now()
		return func(info trace.DriverConnDialDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "done",
					Stringer("endpoint", endpoint),
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					Stringer("endpoint", endpoint),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "driver", "conn", "state", "change")
		endpoint := info.Endpoint
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
			Stringer("state", info.State),
		)
		start := time.Now()
		return func(info trace.DriverConnStateChangeDoneInfo) {
			l.Log(ctx, "done",
				Stringer("endpoint", endpoint),
				latency(start),
				Stringer("state", info.State),
			)
		}
	}
	t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "park")
		endpoint := info.Endpoint
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
		)
		start := time.Now()
		return func(info trace.DriverConnParkDoneInfo) {
			if info.Error == nil {
				l.Log(ctx, "done",
					Stringer("endpoint", endpoint),
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					Stringer("endpoint", endpoint),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "close")
		endpoint := info.Endpoint
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
		)
		start := time.Now()
		return func(info trace.DriverConnCloseDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "done",
					Stringer("endpoint", endpoint),
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					Stringer("endpoint", endpoint),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "invoke")
		endpoint := info.Endpoint
		method := string(info.Method)
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
			String("method", method),
		)
		start := time.Now()
		return func(info trace.DriverConnInvokeDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "done",
					Stringer("endpoint", endpoint),
					String("method", method),
					latency(start),
					Stringer("metadata", metadata(info.Metadata)),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					Stringer("endpoint", endpoint),
					String("method", method),
					latency(start),
					Stringer("metadata", metadata(info.Metadata)),
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
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "new", "stream")
		endpoint := info.Endpoint
		method := string(info.Method)
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
			String("method", method),
		)
		start := time.Now()
		return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "intermediate receive",
					Stringer("endpoint", endpoint),
					String("method", method),
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "intermediate fail",
					Error(info.Error),
					Stringer("endpoint", endpoint),
					String("method", method),
					latency(start),
					version(),
				)
			}
			return func(info trace.DriverConnNewStreamDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, TRACE), "done",
						Stringer("endpoint", endpoint),
						String("method", method),
						latency(start),
						Stringer("metadata", metadata(info.Metadata)),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						Error(info.Error),
						Stringer("endpoint", endpoint),
						String("method", method),
						latency(start),
						Stringer("metadata", metadata(info.Metadata)),
						version(),
					)
				}
			}
		}
	}
	t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "ban")
		endpoint := info.Endpoint
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
			NamedError("cause", info.Cause),
			version(),
		)
		start := time.Now()
		return func(info trace.DriverConnBanDoneInfo) {
			l.Log(WithLevel(ctx, WARN), "done",
				Stringer("endpoint", endpoint),
				latency(start),
				Stringer("state", info.State),
				version(),
			)
		}
	}
	t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
		if d.Details()&trace.DriverConnEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "allow")
		endpoint := info.Endpoint
		l.Log(ctx, "start",
			Stringer("endpoint", endpoint),
		)
		start := time.Now()
		return func(info trace.DriverConnAllowDoneInfo) {
			l.Log(WithLevel(ctx, INFO), "done",
				Stringer("endpoint", endpoint),
				latency(start),
				Stringer("state", info.State),
			)
		}
	}
	t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
		if d.Details()&trace.DriverRepeaterEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "repeater", "wake", "up")
		name := info.Name
		event := info.Event
		l.Log(ctx, "start",
			String("name", name),
			String("event", event),
		)
		start := time.Now()
		return func(info trace.DriverRepeaterWakeUpDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "done",
					String("name", name),
					String("event", event),
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					String("name", name),
					String("event", event),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
		if d.Details()&trace.DriverBalancerEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "init")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.DriverBalancerInitDoneInfo) {
			l.Log(WithLevel(ctx, DEBUG), "done",
				latency(start),
			)
		}
	}
	t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
		if d.Details()&trace.DriverBalancerEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "close")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.DriverBalancerCloseDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "done",
					latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "failed",
					Error(info.Error),
					latency(start),
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
		if d.Details()&trace.DriverBalancerEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "choose", "endpoint")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "done",
					latency(start),
					Stringer("endpoint", info.Endpoint),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "failed",
					Error(info.Error),
					latency(start),
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
		if d.Details()&trace.DriverBalancerEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "update")
		l.Log(ctx, "start",
			Bool("needLocalDC", info.NeedLocalDC),
		)
		start := time.Now()
		return func(info trace.DriverBalancerUpdateDoneInfo) {
			l.Log(WithLevel(ctx, INFO), "done",
				latency(start),
				Stringer("endpoints", endpoints(info.Endpoints)),
				String("detectedLocalDC", info.LocalDC),
			)
		}
	}
	t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
		if d.Details()&trace.DriverCredentialsEvents == 0 {
			return nil
		}
		ctx := with(*info.Context, TRACE, "ydb", "driver", "credentials", "get")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.DriverGetCredentialsDoneInfo) {
			if info.Error == nil {
				l.Log(WithLevel(ctx, TRACE), "done",
					latency(start),
					String("token", Secret(info.Token)),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "done",
					Error(info.Error),
					latency(start),
					String("token", Secret(info.Token)),
					version(),
				)
			}
		}
	}
	return t
}
