package ydb

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/metadata"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/YandexDatabase/ydb-go-sdk/v2/timeutil"
)

type (
	ctxOpTimeoutKey     struct{}
	ctxOpCancelAfterKey struct{}
	ctxOpModeKey        struct{}
	ctxEndpointInfoKey  struct{}

	ctxEndpointInfo struct {
		conn   *conn
		policy ConnUsePolicy
	}
)

type valueOnlyContext struct{ context.Context }

func (valueOnlyContext) Deadline() (deadline time.Time, ok bool) { return }
func (valueOnlyContext) Done() <-chan struct{}                   { return nil }
func (valueOnlyContext) Err() error                              { return nil }

// ContextWithoutDeadline helps to clear derived context from deadline
func ContextWithoutDeadline(ctx context.Context) context.Context {
	return valueOnlyContext{ctx}
}

// WithOperationTimeout returns a copy of parent context in which YDB operation timeout
// parameter is set to d. If parent context timeout is smaller than d, parent context
// is returned.
func WithOperationTimeout(ctx context.Context, d time.Duration) context.Context {
	if cur, ok := ContextOperationTimeout(ctx); ok && d >= cur {
		// The current timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOpTimeoutKey{}, d)
}

// ContextOperationTimeout returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextOperationTimeout(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpTimeoutKey{}).(time.Duration)
	return
}

// WithEndpointInfo returns a copy of parent context with endopint info and custom connection use policy
func WithEndpointInfoAndPolicy(ctx context.Context, endpointInfo EndpointInfo, policy ConnUsePolicy) context.Context {
	if endpointInfo != nil {
		return context.WithValue(ctx, ctxEndpointInfoKey{}, ctxEndpointInfo{
			conn:   endpointInfo.Conn(),
			policy: policy,
		})
	}
	return ctx
}

// WithEndpointInfo returns a copy of parent context with endpoint info and default connection use policy
func WithEndpointInfo(ctx context.Context, endpointInfo EndpointInfo) context.Context {
	if endpointInfo != nil {
		return context.WithValue(ctx, ctxEndpointInfoKey{}, ctxEndpointInfo{
			conn:   endpointInfo.Conn(),
			policy: ConnUseSmart,
		})
	}
	return ctx
}

// WithTraceID returns a copy of parent context with traceID
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metaTraceID, traceID)
}

// WithUserAgent returns a copy of parent context with custom user-agent info
func WithUserAgent(ctx context.Context, userAgent string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metaUserAgent, userAgent)
}

// ContextConn returns the connection and connection use policy
func ContextConn(ctx context.Context) (conn *conn, backoffUseBalancer bool) {
	connInfo, ok := ctx.Value(ctxEndpointInfoKey{}).(ctxEndpointInfo)
	if !ok {
		return nil, true
	}
	return connInfo.conn, connInfo.policy != ConnUseEndpoint
}

// WithOperationCancelAfter returns a copy of parent context in which YDB operation
// cancel after parameter is set to d. If parent context cancelation timeout is smaller
// than d, parent context context is returned.
func WithOperationCancelAfter(ctx context.Context, d time.Duration) context.Context {
	if cur, ok := ContextOperationCancelAfter(ctx); ok && d >= cur {
		// The current cancelation timeout is already smaller than the new one.
		return ctx
	}
	return context.WithValue(ctx, ctxOpCancelAfterKey{}, d)
}

// ContextOperationTimeout returns the timeout within given context after which
// YDB should try to cancel operation and return result regardless of the
// cancelation.
func ContextOperationCancelAfter(ctx context.Context) (d time.Duration, ok bool) {
	d, ok = ctx.Value(ctxOpCancelAfterKey{}).(time.Duration)
	return
}

// WithOperationMode returns a copy of parent context in which YDB operation mode
// parameter is set to m. If parent context mode is set and is not equal to m,
// WithOperationMode will panic.
func WithOperationMode(ctx context.Context, m OperationMode) context.Context {
	if cur, ok := ContextOperationMode(ctx); ok {
		if cur != m {
			panic(fmt.Sprintf(
				"ydb: context already has different operation mode: %v; %v given",
				cur, m,
			))
		}
		return ctx
	}
	return context.WithValue(ctx, ctxOpModeKey{}, m)
}

// ContextOperationMode returns the mode of YDB operation within given context.
func ContextOperationMode(ctx context.Context) (m OperationMode, ok bool) {
	m, ok = ctx.Value(ctxOpModeKey{}).(OperationMode)
	return
}

type OperationMode uint

const (
	OperationModeUnknown OperationMode = iota
	OperationModeSync
	OperationModeAsync
)

func (m OperationMode) String() string {
	switch m {
	case OperationModeSync:
		return "sync"
	case OperationModeAsync:
		return "async"
	default:
		return "unknown"
	}
}
func (m OperationMode) toYDB() Ydb_Operations.OperationParams_OperationMode {
	switch m {
	case OperationModeSync:
		return Ydb_Operations.OperationParams_SYNC
	case OperationModeAsync:
		return Ydb_Operations.OperationParams_ASYNC
	default:
		return Ydb_Operations.OperationParams_OPERATION_MODE_UNSPECIFIED
	}
}

type OperationParams struct {
	Timeout     time.Duration
	CancelAfter time.Duration
	Mode        OperationMode
}

func (p OperationParams) Empty() bool {
	return p.Timeout == 0 && p.CancelAfter == 0 && p.Mode == 0
}

func (p OperationParams) toYDB() *Ydb_Operations.OperationParams {
	if p.Empty() {
		return nil
	}
	return &Ydb_Operations.OperationParams{
		OperationMode:    p.Mode.toYDB(),
		OperationTimeout: timeoutParam(p.Timeout),
		CancelAfter:      timeoutParam(p.CancelAfter),
	}
}

func operationParams(ctx context.Context) (p OperationParams) {
	var hasOpTimeout bool

	p.Mode, _ = ContextOperationMode(ctx)
	p.Timeout, hasOpTimeout = ContextOperationTimeout(ctx)
	p.CancelAfter, _ = ContextOperationCancelAfter(ctx)

	if p.Mode != OperationModeSync {
		return
	}

	deadline, hasDeadline := contextUntilDeadline(ctx)
	if !hasDeadline {
		return
	}

	if hasOpTimeout && p.Timeout <= deadline {
		return
	}

	p.Timeout = deadline

	return
}

func setOperationParams(req interface{}, params OperationParams) {
	x, ok := req.(interface {
		SetOperationParams(*Ydb_Operations.OperationParams)
	})
	if !ok {
		return
	}
	x.SetOperationParams(params.toYDB())
}

func timeoutParam(d time.Duration) *durationpb.Duration {
	if d > 0 {
		return durationpb.New(d)
	}
	return nil
}

func contextUntilDeadline(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if ok {
		return timeutil.Until(deadline), true
	}
	return 0, false
}

type credentialsSourceInfoContextKey struct{}

func WithCredentialsSourceInfo(ctx context.Context, sourceInfo string) context.Context {
	return context.WithValue(ctx, credentialsSourceInfoContextKey{}, sourceInfo)
}

func ContextCredentialsSourceInfo(ctx context.Context) (sourceInfo string, ok bool) {
	sourceInfo, ok = ctx.Value(credentialsSourceInfoContextKey{}).(string)
	return
}
