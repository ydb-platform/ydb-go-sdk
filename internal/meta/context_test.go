package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestContext(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ctx    context.Context //nolint:containedctx
		header string
		values []string
	}{
		{
			name:   "WithApplicationName",
			ctx:    WithApplicationName(context.Background(), "test"),
			header: HeaderApplicationName,
			values: []string{"test"},
		},
		{
			name: "WithApplicationName",
			ctx: WithApplicationName(
				WithApplicationName(
					context.Background(),
					"test1",
				),
				"test2",
			),
			header: HeaderApplicationName,
			values: []string{"test2"},
		},
		{
			name:   "WithTraceID",
			ctx:    WithTraceID(context.Background(), "my-trace-id"),
			header: HeaderTraceID,
			values: []string{"my-trace-id"},
		},
		{
			name:   "WithRequestType",
			ctx:    WithRequestType(context.Background(), "my-request-type"),
			header: HeaderRequestType,
			values: []string{"my-request-type"},
		},
		{
			name:   "WithAllowFeatures",
			ctx:    WithAllowFeatures(context.Background(), "feature-1", "feature-2", "feature-3"),
			header: HeaderClientCapabilities,
			values: []string{"feature-1", "feature-2", "feature-3"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			md, has := metadata.FromOutgoingContext(tt.ctx)
			require.True(t, has)
			require.Equal(t, tt.values, md.Get(tt.header))
		})
	}
}

func TestWithoutAllowFeatures(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithAllowFeatures(ctx, "feature-1", "feature-2", "feature-3")

		ctx = WithoutAllowFeatures(ctx, "feature-1", "feature-3")

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		features := md.Get(HeaderClientCapabilities)
		assert.Contains(t, features, "feature-2")
		assert.NotContains(t, features, "feature-1")
		assert.NotContains(t, features, "feature-3")
	})

	t.Run("only one feature", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithAllowFeatures(ctx, "feature")

		ctx = WithoutAllowFeatures(ctx, "feature")

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		features := md.Get(HeaderClientCapabilities)
		assert.Empty(t, features)
	})

	t.Run("no client capabilities", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithRequestType(ctx, "my-request-type")

		ctx = WithoutAllowFeatures(ctx, "feature-1", "feature-2", "feature-3")

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		assert.Equal(t, md.Get(HeaderRequestType), []string{"my-request-type"})
	})
}
