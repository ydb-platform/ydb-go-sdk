package ydb

import (
	"context"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Operations"
)

type DriverTrace struct {
	DialStart func(DialStartInfo)
	DialDone  func(DialDoneInfo)

	GetConnStart func(GetConnStartInfo)
	GetConnDone  func(GetConnDoneInfo)

	DiscoveryStart func(DiscoveryStartInfo)
	DiscoveryDone  func(DiscoveryDoneInfo)

	OperationStart func(OperationStartInfo)
	OperationWait  func(OperationWaitInfo)
	OperationDone  func(OperationDoneInfo)
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
func (d DriverTrace) getConnDone(ctx context.Context, addr string, err error) {
	x := GetConnDoneInfo{
		Context: ctx,
		Address: addr,
		Error:   err,
	}
	if f := d.GetConnDone; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).GetConnDone; f != nil {
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
func (d DriverTrace) operationStart(ctx context.Context, conn *conn, method string) {
	x := OperationStartInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
	}
	if f := d.OperationStart; f != nil {
		f(x)
	}
	if f := ContextDriverTrace(ctx).OperationStart; f != nil {
		f(x)
	}
}
func (d DriverTrace) operationDone(ctx context.Context, conn *conn, method string, resp Ydb_Operations.GetOperationResponse, err error) {
	x := OperationDoneInfo{
		Context: ctx,
		Address: conn.addr.String(),
		Method:  Method(method),
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

// Method represents rpc method.
type Method string

// Name returns the rpc method name.
func (m Method) Name() string {
	i := strings.LastIndex(string(m), "/")
	if i == -1 {
		return string(m)
	}
	return string(m[i+1:])
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
	}
	OperationWaitInfo struct {
		Context context.Context
		Address string
		Method  Method
		OpID    string
	}
	OperationDoneInfo struct {
		Context context.Context
		Address string
		Method  Method
		OpID    string
		Issues  IssueIterator
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
