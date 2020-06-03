package ydb

import (
	"context"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/api"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Operations"
)

type DriverTrace struct {
	DialStart func(DialStartInfo)
	DialDone  func(DialDoneInfo)

	GetConnStart func(GetConnStartInfo)
	GetConnDone  func(GetConnDoneInfo)

	// Only for background.
	TrackConnStart func(TrackConnStartInfo)
	// Only for background.
	TrackConnDone func(TrackConnDoneInfo)

	GetCredentialsStart func(GetCredentialsStartInfo)
	GetCredentialsDone  func(GetCredentialsDoneInfo)

	DiscoveryStart func(DiscoveryStartInfo)
	DiscoveryDone  func(DiscoveryDoneInfo)

	OperationStart func(OperationStartInfo)
	OperationWait  func(OperationWaitInfo)
	OperationDone  func(OperationDoneInfo)

	StreamStart     func(StreamStartInfo)
	StreamRecvStart func(StreamRecvStartInfo)
	StreamRecvDone  func(StreamRecvDoneInfo)
	StreamDone      func(StreamDoneInfo)
}

func (d DriverTrace) dialStart(ctx context.Context, addr string) {
	x := DialStartInfo{
		Context: ctx,
		Address: addr,
	}
	if f := d.DialStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).DialStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) dialDone(ctx context.Context, addr string, err error) {
	x := DialDoneInfo{
		Context: ctx,
		Address: addr,
		Error:   err,
	}
	if f := d.DialDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).DialDone; f != nil {
		f(x)
	}
}
func (d DriverTrace) getConnStart(ctx context.Context) {
	x := GetConnStartInfo{
		Context: ctx,
	}
	if f := d.GetConnStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).GetConnStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) getConnDone(ctx context.Context, conn *conn, err error) {
	x := GetConnDoneInfo{
		Context: ctx,
		Error:   err,
	}
	if conn != nil {
		x.Address = conn.addr.String()
	}
	if f := d.GetConnDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).GetConnDone; f != nil {
		f(x)
	}
}
func (d DriverTrace) trackConnStart(conn *conn) {
	x := TrackConnStartInfo{
		Address: conn.addr.String(),
	}
	if f := d.TrackConnStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) trackConnDone(conn *conn) {
	x := TrackConnDoneInfo{
		Address: conn.addr.String(),
	}
	if f := d.TrackConnDone; f != nil {
		f(x)
	}
}
func (d DriverTrace) getCredentialsStart(ctx context.Context) {
	x := GetCredentialsStartInfo{
		Context: ctx,
	}
	if f := d.GetCredentialsStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).GetCredentialsStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) getCredentialsDone(ctx context.Context, token bool, err error) {
	x := GetCredentialsDoneInfo{
		Context: ctx,
		Token:   token,
		Error:   err,
	}
	if f := d.GetCredentialsDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).GetCredentialsDone; f != nil {
		f(x)
	}
}
func (d DriverTrace) discoveryStart(ctx context.Context) {
	x := DiscoveryStartInfo{
		Context: ctx,
	}
	if f := d.DiscoveryStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).DiscoveryStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) discoveryDone(ctx context.Context, es []Endpoint, err error) {
	x := DiscoveryDoneInfo{
		Context:   ctx,
		Endpoints: es,
		Error:     err,
	}
	if f := d.DiscoveryDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).DiscoveryDone; f != nil {
		f(x)
	}
}
func (d DriverTrace) operationStart(ctx context.Context, conn *conn, method string, params OperationParams) {
	x := OperationStartInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
		Params:  params,
	}
	if f := d.OperationStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).OperationStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) operationDone(ctx context.Context, conn *conn, method string, params OperationParams, resp Ydb_Operations.GetOperationResponse, err error) {
	x := OperationDoneInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
		Params:  params,
		Error:   err,
	}
	if op := resp.Operation; op != nil {
		x.OpID = op.Id
		x.Issues = IssueIterator(op.Issues)
	}
	if f := d.OperationDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).OperationDone; f != nil {
		f(x)
	}
}

func (d DriverTrace) streamStart(ctx context.Context, conn *conn, method string) {
	x := StreamStartInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
	}
	if f := d.StreamStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).StreamStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) streamDone(ctx context.Context, conn *conn, method string, err error) {
	x := StreamDoneInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
		Error:   err,
	}
	if f := d.StreamDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).StreamDone; f != nil {
		f(x)
	}
}
func (d DriverTrace) streamRecvStart(ctx context.Context, conn *conn, method string) {
	x := StreamRecvStartInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
	}
	if f := d.StreamRecvStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).StreamRecvStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) streamRecvDone(ctx context.Context, conn *conn, method string, resp api.StreamOperationResponse, err error) {
	x := StreamRecvDoneInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
		Error:   err,
	}
	if resp != nil {
		x.Issues = IssueIterator(resp.GetIssues())
	}
	if f := d.StreamRecvDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).StreamRecvDone; f != nil {
		f(x)
	}
}

// Method represents rpc method.
type Method string

// Name returns the rpc method name.
func (m Method) Name() (s string) {
	_, s = m.Split()
	return
}

// Service returns the rpc service name.
func (m Method) Service() (s string) {
	s, _ = m.Split()
	return
}

// Split returns service name and method.
func (m Method) Split() (service, method string) {
	i := strings.LastIndex(string(m), "/")
	if i == -1 {
		return string(m), string(m)
	}
	return strings.TrimPrefix(string(m[:i]), "/"), string(m[i+1:])
}

type (
	DialStartInfo struct {
		Context context.Context
		Address string
	}
	DialDoneInfo struct {
		Context context.Context
		Address string
		Error   error
	}
	GetConnStartInfo struct {
		Context context.Context
	}
	GetConnDoneInfo struct {
		Context context.Context
		Address string
		Error   error
	}
	TrackConnStartInfo struct {
		Address string
	}
	TrackConnDoneInfo struct {
		Address string
	}
	GetCredentialsStartInfo struct {
		Context context.Context
	}
	GetCredentialsDoneInfo struct {
		Context context.Context
		Token   bool
		Error   error
	}
	DiscoveryStartInfo struct {
		Context context.Context
	}
	DiscoveryDoneInfo struct {
		Context   context.Context
		Endpoints []Endpoint
		Error     error
	}
	OperationStartInfo struct {
		Context context.Context
		Address string
		Method  Method
		Params  OperationParams
	}
	OperationWaitInfo struct {
		Context context.Context
		Address string
		Method  Method
		Params  OperationParams
		OpID    string
	}
	OperationDoneInfo struct {
		Context context.Context
		Address string
		Method  Method
		Params  OperationParams
		OpID    string
		Issues  IssueIterator
		Error   error
	}
	StreamStartInfo struct {
		Context context.Context
		Address string
		Method  Method
	}
	StreamRecvStartInfo struct {
		Context context.Context
		Address string
		Method  Method
	}
	StreamRecvDoneInfo struct {
		Context context.Context
		Address string
		Method  Method
		Issues  IssueIterator
		Error   error
	}
	StreamDoneInfo struct {
		Context context.Context
		Address string
		Method  Method
		Error   error
	}
)

func composeDriverTrace(a, b DriverTrace) (c DriverTrace) {
	switch {
	case a.DialStart == nil:
		c.DialStart = b.DialStart
	case b.DialStart == nil:
		c.DialStart = a.DialStart
	default:
		c.DialStart = func(info DialStartInfo) {
			a.DialStart(info)
			b.DialStart(info)
		}
	}
	switch {
	case a.DialDone == nil:
		c.DialDone = b.DialDone
	case b.DialDone == nil:
		c.DialDone = a.DialDone
	default:
		c.DialDone = func(info DialDoneInfo) {
			a.DialDone(info)
			b.DialDone(info)
		}
	}
	switch {
	case a.GetConnStart == nil:
		c.GetConnStart = b.GetConnStart
	case b.GetConnStart == nil:
		c.GetConnStart = a.GetConnStart
	default:
		c.GetConnStart = func(info GetConnStartInfo) {
			a.GetConnStart(info)
			b.GetConnStart(info)
		}
	}
	switch {
	case a.GetConnDone == nil:
		c.GetConnDone = b.GetConnDone
	case b.GetConnDone == nil:
		c.GetConnDone = a.GetConnDone
	default:
		c.GetConnDone = func(info GetConnDoneInfo) {
			a.GetConnDone(info)
			b.GetConnDone(info)
		}
	}
	switch {
	case a.TrackConnStart == nil:
		c.TrackConnStart = b.TrackConnStart
	case b.TrackConnStart == nil:
		c.TrackConnStart = a.TrackConnStart
	default:
		c.TrackConnStart = func(info TrackConnStartInfo) {
			a.TrackConnStart(info)
			b.TrackConnStart(info)
		}
	}
	switch {
	case a.TrackConnDone == nil:
		c.TrackConnDone = b.TrackConnDone
	case b.TrackConnDone == nil:
		c.TrackConnDone = a.TrackConnDone
	default:
		c.TrackConnDone = func(info TrackConnDoneInfo) {
			a.TrackConnDone(info)
			b.TrackConnDone(info)
		}
	}
	switch {
	case a.GetCredentialsStart == nil:
		c.GetCredentialsStart = b.GetCredentialsStart
	case b.GetCredentialsStart == nil:
		c.GetCredentialsStart = a.GetCredentialsStart
	default:
		c.GetCredentialsStart = func(info GetCredentialsStartInfo) {
			a.GetCredentialsStart(info)
			b.GetCredentialsStart(info)
		}
	}
	switch {
	case a.GetCredentialsDone == nil:
		c.GetCredentialsDone = b.GetCredentialsDone
	case b.GetCredentialsDone == nil:
		c.GetCredentialsDone = a.GetCredentialsDone
	default:
		c.GetCredentialsDone = func(info GetCredentialsDoneInfo) {
			a.GetCredentialsDone(info)
			b.GetCredentialsDone(info)
		}
	}
	switch {
	case a.DiscoveryStart == nil:
		c.DiscoveryStart = b.DiscoveryStart
	case b.DiscoveryStart == nil:
		c.DiscoveryStart = a.DiscoveryStart
	default:
		c.DiscoveryStart = func(info DiscoveryStartInfo) {
			a.DiscoveryStart(info)
			b.DiscoveryStart(info)
		}
	}
	switch {
	case a.DiscoveryDone == nil:
		c.DiscoveryDone = b.DiscoveryDone
	case b.DiscoveryDone == nil:
		c.DiscoveryDone = a.DiscoveryDone
	default:
		c.DiscoveryDone = func(info DiscoveryDoneInfo) {
			a.DiscoveryDone(info)
			b.DiscoveryDone(info)
		}
	}
	switch {
	case a.OperationStart == nil:
		c.OperationStart = b.OperationStart
	case b.OperationStart == nil:
		c.OperationStart = a.OperationStart
	default:
		c.OperationStart = func(info OperationStartInfo) {
			a.OperationStart(info)
			b.OperationStart(info)
		}
	}
	switch {
	case a.OperationWait == nil:
		c.OperationWait = b.OperationWait
	case b.OperationWait == nil:
		c.OperationWait = a.OperationWait
	default:
		c.OperationWait = func(info OperationWaitInfo) {
			a.OperationWait(info)
			b.OperationWait(info)
		}
	}
	switch {
	case a.OperationDone == nil:
		c.OperationDone = b.OperationDone
	case b.OperationDone == nil:
		c.OperationDone = a.OperationDone
	default:
		c.OperationDone = func(info OperationDoneInfo) {
			a.OperationDone(info)
			b.OperationDone(info)
		}
	}
	switch {
	case a.StreamStart == nil:
		c.StreamStart = b.StreamStart
	case b.StreamStart == nil:
		c.StreamStart = a.StreamStart
	default:
		c.StreamStart = func(info StreamStartInfo) {
			a.StreamStart(info)
			b.StreamStart(info)
		}
	}
	switch {
	case a.StreamRecvStart == nil:
		c.StreamRecvStart = b.StreamRecvStart
	case b.StreamRecvStart == nil:
		c.StreamRecvStart = a.StreamRecvStart
	default:
		c.StreamRecvStart = func(info StreamRecvStartInfo) {
			a.StreamRecvStart(info)
			b.StreamRecvStart(info)
		}
	}
	switch {
	case a.StreamRecvDone == nil:
		c.StreamRecvDone = b.StreamRecvDone
	case b.StreamRecvDone == nil:
		c.StreamRecvDone = a.StreamRecvDone
	default:
		c.StreamRecvDone = func(info StreamRecvDoneInfo) {
			a.StreamRecvDone(info)
			b.StreamRecvDone(info)
		}
	}
	switch {
	case a.StreamDone == nil:
		c.StreamDone = b.StreamDone
	case b.StreamDone == nil:
		c.StreamDone = a.StreamDone
	default:
		c.StreamDone = func(info StreamDoneInfo) {
			a.StreamDone(info)
			b.StreamDone(info)
		}
	}

	return
}

type driverTraceContextKey struct{}

func WithDriverTrace(ctx context.Context, trace DriverTrace) context.Context {
	return context.WithValue(ctx,
		driverTraceContextKey{},
		composeDriverTrace(
			ContextDriverTrace(ctx), trace,
		),
	)
}

func ContextDriverTrace(ctx context.Context) DriverTrace {
	trace, _ := ctx.Value(driverTraceContextKey{}).(DriverTrace)
	return trace
}
