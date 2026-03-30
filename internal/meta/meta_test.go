package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestMetaContext(t *testing.T) {
	t.Run("RequiredHeaders", func(t *testing.T) {
		m := New(
			"database",
			credentials.NewAccessTokenCredentials("token"),
			&trace.Driver{},
			WithRequestTypeOption("requestType"),
			WithApplicationNameOption("test app"),
		)

		ctx := context.Background()
		ctx = WithTraceID(ctx, "traceID")
		ctx = metadata.AppendToOutgoingContext(ctx, "some-user-header", "some-user-value")

		ctx, err := m.Context(ctx)
		require.NoError(t, err)

		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)

		require.Equal(t, []string{"database"}, md.Get(HeaderDatabase))
		require.Equal(t, []string{"requestType"}, md.Get(HeaderRequestType))
		require.Equal(t, []string{"token"}, md.Get(HeaderTicket))
		require.NotEmpty(t, md.Get(HeaderClientPid))
		require.NotEmpty(t, md.Get(HeaderClientPid)[0])
		require.Equal(t, []string{"test app"}, md.Get(HeaderApplicationName))
		require.Equal(t, []string{"traceID"}, md.Get(HeaderTraceID))
		require.Equal(t, []string{version.FullVersion}, md.Get(HeaderVersion))
		require.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
	})

	t.Run("BuildInfoSingleEntry", func(t *testing.T) {
		m := New(
			"database",
			nil,
			&trace.Driver{},
			WithBuildInfo("database/sql", "1.2.3"),
		)

		ctx, err := m.Context(context.Background())
		require.NoError(t, err)

		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)
		require.Equal(t, []string{version.FullVersion + ";database/sql/1.2.3"}, md.Get(HeaderVersion))
	})

	t.Run("BuildInfoDeduplication", func(t *testing.T) {
		m := New(
			"database",
			nil,
			&trace.Driver{},
			WithBuildInfo("database/sql", "1.2.3"),
			WithBuildInfo("database/sql", "1.2.3"),
		)

		ctx, err := m.Context(context.Background())
		require.NoError(t, err)

		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)
		require.Equal(t, []string{version.FullVersion + ";database/sql/1.2.3"}, md.Get(HeaderVersion))
	})

	t.Run("BuildInfoMultipleEntries", func(t *testing.T) {
		m := New(
			"database",
			nil,
			&trace.Driver{},
			WithBuildInfo("my-framework", "2.0.0"),
			WithBuildInfo("database/sql", "1.0.0"),
		)

		ctx, err := m.Context(context.Background())
		require.NoError(t, err)

		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)

		headerValues := md.Get(HeaderVersion)
		require.Len(t, headerValues, 1)
		require.Contains(t, headerValues[0], version.FullVersion+";database/sql/1.0.0;my-framework/2.0.0")
	})

	t.Run("BuildInfoAppendBuildInfo", func(t *testing.T) {
		m := New(
			"database",
			nil,
			&trace.Driver{},
			WithBuildInfo("database/sql", "1.2.3"),
		)
		m.AppendBuildInfo("database/sql", "1.2.4")

		ctx, err := m.Context(context.Background())
		require.NoError(t, err)

		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)
		require.Equal(t, []string{version.FullVersion + ";database/sql/1.2.4"}, md.Get(HeaderVersion))
	})
}
