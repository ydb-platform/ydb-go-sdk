package spans

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func traceparent(traceID string, spanID string) string {
	b := xstring.Buffer()
	defer b.Free()

	b.WriteString("00-")
	b.WriteString(traceID)
	b.WriteByte('-')
	b.WriteString(spanID)
	b.WriteString("-01")

	return b.String()
}

// driver makes driver with publishing traces
func driver(adapter Adapter) trace.Driver { //nolint:gocyclo,funlen
	return trace.Driver{
		OnRepeaterWakeUp: func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			if adapter.Details()&trace.DriverRepeaterEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnDial: func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
			if adapter.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			if traceID, valid := adapter.SpanFromContext(*info.Context).TraceID(); valid {
				*info.Context = meta.WithTraceParent(*info.Context, traceID)
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.DriverConnDialDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnConnInvoke: func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			if adapter.Details()&trace.DriverConnEvents == 0 {
				return nil
			}

			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("address", safeAddress(info.Endpoint)),
				kv.String("method", string(info.Method)),
			)

			if id, valid := start.ID(); valid {
				if traceID, valid := start.TraceID(); valid {
					*info.Context = meta.WithTraceParent(*info.Context, traceparent(traceID, id))
				}
			}

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
			if adapter.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			*info.Context = withGrpcStreamMsgCounters(*info.Context)
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("address", safeAddress(info.Endpoint)),
				kv.String("method", string(info.Method)),
			)

			if id, valid := start.ID(); valid {
				if traceID, valid := start.TraceID(); valid {
					*info.Context = meta.WithTraceParent(*info.Context, traceparent(traceID, id))
				}
			}

			return func(info trace.DriverConnNewStreamDoneInfo) {
				finish(start, info.Error,
					kv.String("state", safeStringer(info.State)),
				)
			}
		},
		OnConnStreamRecvMsg: func(info trace.DriverConnStreamRecvMsgStartInfo) func(trace.DriverConnStreamRecvMsgDoneInfo) {
			if adapter.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			start := adapter.SpanFromContext(*info.Context)
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
			if adapter.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			start := adapter.SpanFromContext(*info.Context)
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
			if adapter.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}

			var (
				call  = info.Call
				start = adapter.SpanFromContext(*info.Context)
			)

			return func(info trace.DriverConnStreamCloseSendDoneInfo) {
				if info.Error != nil {
					start.Log(call.String(), kv.Error(info.Error))
				}
			}
		},
		OnConnStreamFinish: func(info trace.DriverConnStreamFinishInfo) {
			if adapter.Details()&trace.DriverConnStreamEvents == 0 {
				return
			}

			var (
				call     = info.Call
				start    = adapter.SpanFromContext(info.Context)
				counters = grpcStreamMsgCountersFromContext(info.Context)
			)

			attributes := make([]kv.KeyValue, 0, 2)
			if counters != nil {
				attributes = append(attributes,
					kv.Int64("received_messages", counters.receivedMessages()),
					kv.Int64("sent_messages", counters.sentMessages()),
				)
			}

			if info.Error != nil {
				attributes = append(attributes, kv.Error(info.Error))
			}

			start.Log(call.String(), attributes...)
		},
		OnConnPark: func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			if adapter.Details()&trace.DriverConnEvents == 0 {
				return nil
			}

			var (
				call  = info.Call
				start = adapter.SpanFromContext(*info.Context)
			)

			return func(info trace.DriverConnParkDoneInfo) {
				if info.Error != nil {
					start.Log(call.String(), kv.Error(info.Error))
				} else {
					start.Log(call.String())
				}
			}
		},
		OnConnClose: func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			if adapter.Details()&trace.DriverConnEvents == 0 {
				return nil
			}

			var (
				call  = info.Call
				start = adapter.SpanFromContext(*info.Context)
			)

			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error != nil {
					start.Log(call.String(), kv.Error(info.Error))
				} else {
					start.Log(call.String())
				}
			}
		},
		OnConnBan: func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			if adapter.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			s := adapter.SpanFromContext(*info.Context)
			s.Log(info.Call.String(),
				kv.String("cause", safeError(info.Cause)),
			)

			return nil
		},
		OnConnStateChange: func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			if adapter.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			s := adapter.SpanFromContext(*info.Context)
			oldState := safeStringer(info.State)
			functionID := info.Call.String()

			return func(info trace.DriverConnStateChangeDoneInfo) {
				s.Log(functionID,
					kv.String("old state", oldState),
					kv.String("new state", safeStringer(info.State)),
				)
			}
		},
		OnBalancerInit: func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			if adapter.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("name", info.Name),
			)

			return func(info trace.DriverBalancerInitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnBalancerClusterDiscoveryAttempt: func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
			trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
		) {
			if adapter.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("address", info.Address),
			)

			return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnBalancerUpdate: func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
			if adapter.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(adapter, info.Context,
				info.Call.String(),
				kv.String("database", info.Database),
				kv.Bool("need_local_dc", info.NeedLocalDC),
			)

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
				start.Log(fmt.Sprintf("endpoints=%v", endpoints))
				start.Log(fmt.Sprintf("added=%v", added))
				start.Log(fmt.Sprintf("dropped=%v", dropped))
				start.End(
					kv.String("local_dc", info.LocalDC),
				)
			}
		},
		OnBalancerChooseEndpoint: func(
			info trace.DriverBalancerChooseEndpointStartInfo,
		) func(
			trace.DriverBalancerChooseEndpointDoneInfo,
		) {
			if adapter.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			parent := adapter.SpanFromContext(*info.Context)
			functionID := info.Call.String()

			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error != nil {
					parent.Error(info.Error)
				} else {
					parent.Log(functionID,
						kv.String("address", safeAddress(info.Endpoint)),
						kv.String("nodeID", safeNodeID(info.Endpoint)),
					)
				}
			}
		},
		OnGetCredentials: func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			if adapter.Details()&trace.DriverCredentialsEvents == 0 {
				return nil
			}
			parent := adapter.SpanFromContext(*info.Context)
			functionID := info.Call.String()

			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error != nil {
					parent.Error(info.Error)
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
					parent.Log(functionID,
						kv.String("token", mask.String()),
					)
				}
			}
		},
		OnInit: func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			if adapter.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
				kv.String("endpoint", info.Endpoint),
				kv.String("database", info.Database),
				kv.Bool("secure", info.Secure),
			)

			return func(info trace.DriverInitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnClose: func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			if adapter.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
			)

			return func(info trace.DriverCloseDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnPoolNew: func(info trace.DriverConnPoolNewStartInfo) func(trace.DriverConnPoolNewDoneInfo) {
			if adapter.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				adapter,
				info.Context,
				info.Call.String(),
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
