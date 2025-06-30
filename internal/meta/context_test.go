package meta

import (
	"context"
	"testing"

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
