package log

import (
	"context"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Coordination makes trace.Coordination with logging events from details
func Coordination(l Logger, d trace.Detailer, opts ...Option) (t trace.Coordination) {
	return internalCoordination(wrapLogger(l, opts...), d)
}

//nolint:funlen
func internalCoordination(
	l *wrapper, //nolint:interfacer
	d trace.Detailer,
) trace.Coordination {
	return trace.Coordination{
		OnNew: func(info trace.CoordinationNewStartInfo) func(trace.CoordinationNewDoneInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "coordination", "new")
			l.Log(ctx, "starting coordination client...")
			start := time.Now()

			return func(info trace.CoordinationNewDoneInfo) {
				l.Log(WithLevel(ctx, INFO), "coordination client started",
					kv.Latency(start),
					kv.Version(),
				)
			}
		},
		OnCreateNode: func(info trace.CoordinationCreateNodeStartInfo) func(trace.CoordinationCreateNodeDoneInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "coordination", "node", "create")
			l.Log(ctx, "starting coordination node...",
				kv.String("path", info.Path),
			)
			start := time.Now()

			return func(info trace.CoordinationCreateNodeDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, INFO), "coordination node started",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "coordination node start failed",
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnAlterNode: func(info trace.CoordinationAlterNodeStartInfo) func(trace.CoordinationAlterNodeDoneInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "coordination", "node", "alter")
			l.Log(ctx, "starting alter coordination node...",
				kv.String("path", info.Path),
			)
			start := time.Now()

			return func(info trace.CoordinationAlterNodeDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, INFO), "alter coordination node done",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "alter coordination node failed",
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnDropNode: func(info trace.CoordinationDropNodeStartInfo) func(trace.CoordinationDropNodeDoneInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "coordination", "node", "drop")
			l.Log(ctx, "starting drop coordination node...",
				kv.String("path", info.Path),
			)
			start := time.Now()

			return func(info trace.CoordinationDropNodeDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, INFO), "coordination node dropped",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "coordination node drop failed",
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnDescribeNode: func(info trace.CoordinationDescribeNodeStartInfo) func(trace.CoordinationDescribeNodeDoneInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "coordination", "node", "describe")
			l.Log(ctx, "starting describe coordination node...",
				kv.String("path", info.Path),
			)
			start := time.Now()

			return func(info trace.CoordinationDescribeNodeDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, INFO), "describe node started",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "describe node start failed",
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnSession: func(info trace.CoordinationSessionStartInfo) func(trace.CoordinationSessionDoneInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "coordination", "node", "describe")
			l.Log(ctx, "starting create coordination session...")
			start := time.Now()

			return func(info trace.CoordinationSessionDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, INFO), "coordination session created",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "create coordination session failed",
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnClose: func(info trace.CoordinationCloseStartInfo) func(trace.CoordinationCloseDoneInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "coordination", "close")
			l.Log(ctx, "start closing coordination client...")
			start := time.Now()

			return func(info trace.CoordinationCloseDoneInfo) {
				if info.Error == nil {
					l.Log(WithLevel(ctx, INFO), "coordination client closed",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "close coordination client failed",
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnSessionNewStream: func(
			info trace.CoordinationSessionNewStreamStartInfo,
		) func(
			info trace.CoordinationSessionNewStreamDoneInfo,
		) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stream", "new")
			l.Log(ctx, "starting new coordination session stam...")
			start := time.Now()

			return func(info trace.CoordinationSessionNewStreamDoneInfo) {
				l.Log(ctx, "new coordination session stream started",
					kv.Latency(start),
					kv.Error(info.Error),
					kv.Version(),
				)
			}
		},
		OnSessionStarted: func(info trace.CoordinationSessionStartedInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "started")
			l.Log(ctx, "coordination session started",
				kv.String("sessionID", strconv.FormatUint(info.SessionID, 10)),
				kv.String("expectedSessionID", strconv.FormatUint(info.SessionID, 10)),
			)
		},
		OnSessionStartTimeout: func(info trace.CoordinationSessionStartTimeoutInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "start", "timeout")
			l.Log(ctx, "coordination session start timeout",
				kv.Stringer("timeout", info.Timeout),
			)
		},
		OnSessionKeepAliveTimeout: func(info trace.CoordinationSessionKeepAliveTimeoutInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "keepAlive", "timeout")
			l.Log(ctx, "coordination session keep alive timeout",
				kv.Stringer("timeout", info.Timeout),
				kv.Stringer("lastGoodResponseTime", info.LastGoodResponseTime),
			)
		},
		OnSessionStopped: func(info trace.CoordinationSessionStoppedInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stopped")
			l.Log(ctx, "coordination session stopped",
				kv.String("sessionID", strconv.FormatUint(info.SessionID, 10)),
				kv.String("expectedSessionID", strconv.FormatUint(info.SessionID, 10)),
			)
		},
		OnSessionStopTimeout: func(info trace.CoordinationSessionStopTimeoutInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stop", "timeout")
			l.Log(ctx, "coordination session stop timeout",
				kv.Stringer("timeout", info.Timeout),
			)
		},
		OnSessionClientTimeout: func(info trace.CoordinationSessionClientTimeoutInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "client", "timeout")
			l.Log(ctx, "coordination session client timeout",
				kv.Stringer("timeout", info.Timeout),
				kv.Stringer("lastGoodResponseTime", info.LastGoodResponseTime),
			)
		},
		OnSessionServerExpire: func(info trace.CoordinationSessionServerExpireInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "server", "expire")
			l.Log(ctx, "coordination session server expire",
				kv.Stringer("failure", info.Failure),
			)
		},
		OnSessionServerError: func(info trace.CoordinationSessionServerErrorInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "server", "error")
			l.Log(ctx, "coordination session server error",
				kv.Stringer("failure", info.Failure),
			)
		},
		OnSessionReceive: func(
			info trace.CoordinationSessionReceiveStartInfo,
		) func(
			info trace.CoordinationSessionReceiveDoneInfo,
		) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "receive")
			l.Log(ctx, "starting coordination session receive")
			start := time.Now()

			return func(info trace.CoordinationSessionReceiveDoneInfo) {
				l.Log(ctx, "coordination session received",
					kv.Latency(start),
					kv.Error(info.Error),
					kv.Stringer("response", info.Response),
					kv.Version(),
				)
			}
		},
		OnSessionReceiveUnexpected: func(info trace.CoordinationSessionReceiveUnexpectedInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "receive", "unexpected")
			l.Log(ctx, "coordination session received unexpected",
				kv.Stringer("response", info.Response),
			)
		},
		OnSessionStop: func(info trace.CoordinationSessionStopInfo) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stop")
			l.Log(ctx, "coordination session stopped",
				kv.String("sessionID", strconv.FormatUint(info.SessionID, 10)),
			)
		},
		OnSessionStart: func(
			info trace.CoordinationSessionStartStartInfo,
		) func(
			info trace.CoordinationSessionStartDoneInfo,
		) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "start")
			l.Log(ctx, "starting coordination session...")
			start := time.Now()

			return func(info trace.CoordinationSessionStartDoneInfo) {
				l.Log(ctx, "coordination session start done",
					kv.Latency(start),
					kv.Error(info.Error),
					kv.Version(),
				)
			}
		},
		OnSessionSend: func(
			info trace.CoordinationSessionSendStartInfo,
		) func(
			info trace.CoordinationSessionSendDoneInfo,
		) {
			if d.Details()&trace.CoordinationEvents == 0 {
				return nil
			}
			ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "send")
			l.Log(ctx, "start",
				kv.Stringer("request", info.Request),
			)
			start := time.Now()

			return func(info trace.CoordinationSessionSendDoneInfo) {
				l.Log(ctx, "done",
					kv.Latency(start),
					kv.Error(info.Error),
					kv.Version(),
				)
			}
		},
	}
}
