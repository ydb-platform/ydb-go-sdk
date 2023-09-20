package log

import (
	"context"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Coordination makes trace.Coordination with logging events from details
func Coordination(l Logger, d trace.Detailer, opts ...Option) (t trace.Coordination) {
	return internalCoordination(wrapLogger(l, opts...), d)
}

func internalCoordination(l *wrapper, d trace.Detailer) (t trace.Coordination) {
	t.OnStreamNew = func(
		info trace.CoordinationStreamNewStartInfo,
	) func(
		info trace.CoordinationStreamNewDoneInfo,
	) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stream", "new")
		l.Log(ctx, "stream")
		start := time.Now()
		return func(info trace.CoordinationStreamNewDoneInfo) {
			l.Log(ctx, "done",
				latencyField(start),
				Error(info.Error),
				versionField())
		}
	}

	t.OnSessionStarted = func(info trace.CoordinationSessionStartedInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "started")
		l.Log(ctx, "",
			String("sessionID", strconv.FormatUint(info.SessionID, 10)),
			String("expectedSessionID", strconv.FormatUint(info.SessionID, 10)),
		)
	}

	t.OnSessionStartTimeout = func(info trace.CoordinationSessionStartTimeoutInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "start", "timeout")
		l.Log(ctx, "",
			Stringer("timeout", info.Timeout),
		)
	}

	t.OnSessionKeepAliveTimeout = func(info trace.CoordinationSessionKeepAliveTimeoutInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "keepAlive", "timeout")
		l.Log(ctx, "",
			Stringer("timeout", info.Timeout),
			Stringer("lastGoodResponseTime", info.LastGoodResponseTime),
		)
	}

	t.OnSessionStopped = func(info trace.CoordinationSessionStoppedInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stopped")
		l.Log(ctx, "",
			String("sessionID", strconv.FormatUint(info.SessionID, 10)),
			String("expectedSessionID", strconv.FormatUint(info.SessionID, 10)),
		)
	}

	t.OnSessionStopTimeout = func(info trace.CoordinationSessionStopTimeoutInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stop", "timeout")
		l.Log(ctx, "",
			Stringer("timeout", info.Timeout),
		)
	}

	t.OnSessionClientTimeout = func(info trace.CoordinationSessionClientTimeoutInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "client", "timeout")
		l.Log(ctx, "",
			Stringer("timeout", info.Timeout),
			Stringer("lastGoodResponseTime", info.LastGoodResponseTime),
		)
	}

	t.OnSessionServerExpire = func(info trace.CoordinationSessionServerExpireInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "server", "expire")
		l.Log(ctx, "",
			Stringer("failure", info.Failure),
		)
	}

	t.OnSessionServerError = func(info trace.CoordinationSessionServerErrorInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "server", "error")
		l.Log(ctx, "",
			Stringer("failure", info.Failure),
		)
	}

	t.OnSessionReceive = func(
		info trace.CoordinationSessionReceiveStartInfo,
	) func(
		info trace.CoordinationSessionReceiveDoneInfo,
	) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "receive")
		l.Log(ctx, "receive")
		start := time.Now()
		return func(info trace.CoordinationSessionReceiveDoneInfo) {
			l.Log(ctx, "done",
				latencyField(start),
				Error(info.Error),
				Stringer("response", info.Response),
				versionField())
		}
	}

	t.OnSessionReceiveUnexpected = func(info trace.CoordinationSessionReceiveUnexpectedInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "receive", "unexpected")
		l.Log(ctx, "",
			Stringer("response", info.Response),
		)
	}

	t.OnSessionStop = func(info trace.CoordinationSessionStopInfo) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "stop")
		l.Log(ctx, "",
			String("sessionID", strconv.FormatUint(info.SessionID, 10)),
		)
	}

	t.OnSessionStart = func(
		info trace.CoordinationSessionStartStartInfo,
	) func(
		info trace.CoordinationSessionStartDoneInfo,
	) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "start")
		l.Log(ctx, "start")
		start := time.Now()
		return func(info trace.CoordinationSessionStartDoneInfo) {
			l.Log(ctx, "done",
				latencyField(start),
				Error(info.Error),
				versionField())
		}
	}

	t.OnSessionSend = func(
		info trace.CoordinationSessionSendStartInfo,
	) func(
		info trace.CoordinationSessionSendDoneInfo,
	) {
		if d.Details()&trace.CoordinationEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "coordination", "session", "send")
		l.Log(ctx, "start",
			Stringer("request", info.Request),
		)
		start := time.Now()
		return func(info trace.CoordinationSessionSendDoneInfo) {
			l.Log(ctx, "done",
				latencyField(start),
				Error(info.Error),
				versionField())
		}
	}

	return t
}
