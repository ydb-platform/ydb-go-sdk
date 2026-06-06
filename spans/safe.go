package spans

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiface"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func safeAddress(a interface{ Address() string }) string {
	if xiface.IsNil(a) {
		return ""
	}

	return a.Address()
}

func safeNodeID(n interface{ NodeID() uint32 }) string {
	if xiface.IsNil(n) {
		return "0"
	}

	return strconv.FormatUint(uint64(n.NodeID()), 10)
}

func safeNodeIDInt64(n interface{ NodeID() uint32 }) int64 {
	if xiface.IsNil(n) {
		return 0
	}

	return int64(n.NodeID())
}

func safeID(id interface{ ID() string }) string {
	if xiface.IsNil(id) {
		return ""
	}

	return id.ID()
}

func safeStatus(s interface{ Status() string }) string {
	if xiface.IsNil(s) {
		return ""
	}

	return s.Status()
}

func safeStringer(s fmt.Stringer) string {
	if xiface.IsNil(s) {
		return ""
	}

	return s.String()
}

func safeError(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func safeErr(err interface{ Err() error }) error {
	if xiface.IsNil(err) {
		return nil
	}

	return err.Err()
}

func safeCall(c trace.Call) string {
	return safeStringer(c)
}

func safeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return ctx
}

func safeContextPtr(ctx *context.Context) context.Context {
	if ctx == nil || *ctx == nil {
		return context.Background()
	}

	return *ctx
}

func withContextPtr(ctx *context.Context, fn func(context.Context) context.Context) {
	if ctx == nil {
		return
	}
	*ctx = fn(safeContextPtr(ctx))
}

func withCallContext(ctx *context.Context, call trace.Call) {
	withContextPtr(ctx, func(c context.Context) context.Context {
		return withFunctionID(c, safeCall(call))
	})
}

func isNil(v any) bool {
	return xiface.IsNil(v)
}
