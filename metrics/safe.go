package metrics

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiface"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func isNil(v any) bool {
	return xiface.IsNil(v)
}

func safeEndpointAddress(e trace.EndpointInfo) string {
	if isNil(e) {
		return ""
	}

	return e.Address()
}

func safeEndpointNodeID(e trace.EndpointInfo) uint32 {
	if isNil(e) {
		return 0
	}

	return e.NodeID()
}

func safeSessionNodeID(s trace.SessionInfo) uint32 {
	if isNil(s) {
		return 0
	}

	return s.NodeID()
}

func safeTxID(tx trace.TxInfo) string {
	if isNil(tx) {
		return ""
	}

	return tx.ID()
}

func safeContextPtr(ctx *context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	if isNil(*ctx) {
		return context.Background()
	}

	return *ctx
}
