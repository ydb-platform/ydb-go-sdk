package otel

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// driver makes driver with publishing traces
func driver(cfg Config) trace.Driver { //nolint:gocyclo,funlen
	return trace.Driver{
		OnRepeaterWakeUp: func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			if cfg.Details()&trace.DriverRepeaterEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnDial: func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
			if cfg.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			*info.Context = meta.WithTraceParent(*info.Context,
				cfg.SpanFromContext(*info.Context).TraceID(),
			)
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DriverConnDialDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnInvoke: func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			if cfg.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			*info.Context = meta.WithTraceParent(*info.Context,
				cfg.SpanFromContext(*info.Context).TraceID(),
			)
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("address", safeAddress(info.Endpoint)),
				kv.String("method", string(info.Method)),
			)

			return func(info trace.DriverConnInvokeDoneInfo) {
				fields := []KeyValue{
					kv.String("opID", info.OpID),
					kv.String("state", safeStringer(info.State)),
				}
				if len(info.Issues) > 0 {
					issues := make([]string, len(info.Issues))
					for i, issue := range info.Issues {
						issues[i] = fmt.Sprintf("%+v", issue)
					}
					fields = append(fields,
						kv.Strings("issues", issues),
					)
				}
				finish(
					start,
					info.Error,
					fields...,
				)
			}
		},
		OnConnNewStream: func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamDoneInfo) {
			if cfg.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			*info.Context = withGrpcStreamMsgCounters(*info.Context)
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("address", safeAddress(info.Endpoint)),
				kv.String("method", string(info.Method)),
			)
			*info.Context = meta.WithTraceParent(*info.Context,
				start.TraceID(),
			)

			return func(info trace.DriverConnNewStreamDoneInfo) {
				finish(start, info.Error,
					kv.String("state", safeStringer(info.State)),
				)
			}
		},
		OnConnStreamRecvMsg: func(info trace.DriverConnStreamRecvMsgStartInfo) func(trace.DriverConnStreamRecvMsgDoneInfo) {
			if cfg.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			start := cfg.SpanFromContext(*info.Context)
			counters := grpcStreamMsgCountersFromContext(*info.Context)
			if counters != nil {
				counters.updateReceivedMessages()
			}

			return func(info trace.DriverConnStreamRecvMsgDoneInfo) {
				if skipEOF(info.Error) != nil {
					logError(start, info.Error)
				}
			}
		},
		OnConnStreamSendMsg: func(info trace.DriverConnStreamSendMsgStartInfo) func(trace.DriverConnStreamSendMsgDoneInfo) {
			if cfg.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			start := cfg.SpanFromContext(*info.Context)
			counters := grpcStreamMsgCountersFromContext(*info.Context)
			if counters != nil {
				counters.updateSentMessages()
			}

			return func(info trace.DriverConnStreamSendMsgDoneInfo) {
				if info.Error != nil {
					logError(start, info.Error)
				}
			}
		},
		OnConnStreamCloseSend: func(info trace.DriverConnStreamCloseSendStartInfo) func(
			trace.DriverConnStreamCloseSendDoneInfo,
		) {
			if cfg.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DriverConnStreamCloseSendDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnConnStreamFinish: func(info trace.DriverConnStreamFinishInfo) {
			if cfg.Details()&trace.DriverConnStreamEvents == 0 {
				return
			}
			start := childSpanWithReplaceCtx(
				cfg,
				&info.Context,
				info.Call.FunctionID(),
			)
			counters := grpcStreamMsgCountersFromContext(info.Context)

			attributes := make([]kv.KeyValue, 0, 2)
			if counters != nil {
				attributes = append(attributes,
					kv.Int64("received_messages", counters.receivedMessages()),
					kv.Int64("sent_messages", counters.sentMessages()),
				)
			}
			finish(start, info.Error, attributes...)
		},
		OnConnPark: func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			if cfg.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("address", safeAddress(info.Endpoint)),
			)

			return func(info trace.DriverConnParkDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnClose: func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			if cfg.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("address", safeAddress(info.Endpoint)),
			)

			return func(info trace.DriverConnCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnBan: func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			if cfg.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			s := cfg.SpanFromContext(*info.Context)
			s.Msg(info.Call.FunctionID(),
				kv.String("cause", safeError(info.Cause)),
			)

			return nil
		},
		OnConnStateChange: func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			if cfg.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			s := cfg.SpanFromContext(*info.Context)
			oldState := safeStringer(info.State)
			functionID := info.Call.FunctionID()

			return func(info trace.DriverConnStateChangeDoneInfo) {
				s.Msg(functionID,
					kv.String("old state", oldState),
					kv.String("new state", safeStringer(info.State)),
				)
			}
		},
		OnBalancerInit: func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			if cfg.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("name", info.Name),
			)

			return func(info trace.DriverBalancerInitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnBalancerClusterDiscoveryAttempt: func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
			trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
		) {
			if cfg.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnBalancerUpdate: func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
			if cfg.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			needLocalDC := info.NeedLocalDC
			functionID := info.Call.FunctionID()
			s := cfg.SpanFromContext(*info.Context)

			return func(info trace.DriverBalancerUpdateDoneInfo) {
				var (
					endpoints = make([]string, len(info.Endpoints))
					added     = make([]string, len(info.Added))
					dropped   = make([]string, len(info.Dropped))
				)
				for i, e := range info.Endpoints {
					endpoints[i] = e.String()
				}
				for i, e := range info.Added {
					added[i] = e.String()
				}
				for i, e := range info.Dropped {
					dropped[i] = e.String()
				}
				s.Msg(functionID,
					kv.String("local_dc", info.LocalDC),
					kv.Strings("endpoints", endpoints),
					kv.Strings("added", added),
					kv.Strings("dropped", dropped),
					kv.Bool("need_local_dc", needLocalDC),
				)
			}
		},
		OnBalancerChooseEndpoint: func(
			info trace.DriverBalancerChooseEndpointStartInfo,
		) func(
			trace.DriverBalancerChooseEndpointDoneInfo,
		) {
			if cfg.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			parent := cfg.SpanFromContext(*info.Context)
			functionID := info.Call.FunctionID()

			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error != nil {
					parent.Msg(functionID,
						kv.Error(info.Error),
					)
				} else {
					parent.Msg(functionID,
						kv.String("address", safeAddress(info.Endpoint)),
						kv.String("nodeID", safeNodeID(info.Endpoint)),
					)
				}
			}
		},
		OnGetCredentials: func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			if cfg.Details()&trace.DriverCredentialsEvents == 0 {
				return nil
			}
			parent := cfg.SpanFromContext(*info.Context)
			functionID := info.Call.FunctionID()

			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error != nil {
					parent.Msg(functionID,
						kv.Error(info.Error),
					)
				} else {
					var mask bytes.Buffer
					if len(info.Token) > 16 {
						mask.WriteString(info.Token[:4])
						mask.WriteString("****")
						mask.WriteString(info.Token[len(info.Token)-4:])
					} else {
						mask.WriteString("****")
					}
					mask.WriteString(fmt.Sprintf("(CRC-32c: %08X)", crc32.Checksum([]byte(info.Token), crc32.IEEETable)))
					parent.Msg(functionID,
						kv.String("token", mask.String()),
					)
				}
			}
		},
		OnInit: func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			if cfg.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("endpoint", info.Endpoint),
				kv.String("database", info.Database),
				kv.Bool("secure", info.Secure),
			)

			return func(info trace.DriverInitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnClose: func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			if cfg.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DriverCloseDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnPoolNew: func(info trace.DriverConnPoolNewStartInfo) func(trace.DriverConnPoolNewDoneInfo) {
			if cfg.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.DriverConnPoolNewDoneInfo) {
				start.End()
			}
		},
	}
}

type grpcStreamMsgCountersKey struct{}

func withGrpcStreamMsgCounters(ctx context.Context) context.Context {
	return context.WithValue(ctx, grpcStreamMsgCountersKey{}, &grpcStreamMsgCounters{})
}

func grpcStreamMsgCountersFromContext(ctx context.Context) *grpcStreamMsgCounters {
	value, ok := ctx.Value(grpcStreamMsgCountersKey{}).(*grpcStreamMsgCounters)
	if !ok {
		return nil
	}

	return value
}

type grpcStreamMsgCounters struct {
	sent     atomic.Int64
	received atomic.Int64
}

func (c *grpcStreamMsgCounters) updateSentMessages() {
	c.sent.Add(1)
}

func (c *grpcStreamMsgCounters) updateReceivedMessages() {
	c.received.Add(1)
}

func (c *grpcStreamMsgCounters) sentMessages() int64 {
	return c.sent.Load()
}

func (c *grpcStreamMsgCounters) receivedMessages() int64 {
	return c.received.Load()
}
