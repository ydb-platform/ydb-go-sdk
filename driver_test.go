package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"google.golang.org/grpc/metadata"
)

func Test_balancerWithMeta_DisableSessionBalancer(t *testing.T) {
	t.Run("disable session balancer", func(t *testing.T) {
		b := &balancerWithMeta{}
		md := &meta.Meta{}

		b.DisableSessionBalancer()

		opt := b.optionSessionBalancerForMethod(Ydb_Query_V1.QueryService_CreateSession_FullMethodName)
		opt(md)
		ctx, _ := md.Context(context.Background())
		require.Empty(t, extractYDBCapabilities(ctx))
	})

	t.Run("enable session balancer", func(t *testing.T) {
		b := &balancerWithMeta{}
		md := &meta.Meta{}

		// do nothing (no flags)

		opt := b.optionSessionBalancerForMethod(Ydb_Query_V1.QueryService_CreateSession_FullMethodName)
		opt(md)
		ctx, _ := md.Context(context.Background())
		require.Equal(t, []string{meta.HintSessionBalancer}, extractYDBCapabilities(ctx))
	})
}

func extractYDBCapabilities(mdCtx context.Context) []string {
	md, _ := metadata.FromOutgoingContext(mdCtx)
	return md.Get("x-ydb-client-capabilities")
}
