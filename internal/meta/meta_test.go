package meta

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestVersionHeader(t *testing.T) {
	for i, tt := range []struct {
		buildInfo     *xsync.Map[string, string]
		versionHeader string
	}{
		{
			buildInfo: func() *xsync.Map[string, string] {
				return &xsync.Map[string, string]{}
			}(),
			versionHeader: version.FullVersion,
		},
		{
			buildInfo: func() *xsync.Map[string, string] {
				m := &xsync.Map[string, string]{}
				m.Set("testFramework", "0.0.0")

				return m
			}(),
			versionHeader: version.FullVersion + ";testFramework/0.0.0",
		},
		{
			buildInfo: func() *xsync.Map[string, string] {
				m := &xsync.Map[string, string]{}
				m.Set("testFramework1", "0.0.1")
				m.Set("testFramework2", "0.0.2")

				return m
			}(),
			versionHeader: version.FullVersion + ";testFramework1/0.0.1;testFramework2/0.0.2",
		},
		{
			buildInfo: func() *xsync.Map[string, string] {
				m := &xsync.Map[string, string]{}
				m.Set("1testFramework", "0.0.1")
				m.Set("2testFramework", "0.0.2")

				return m
			}(),
			versionHeader: version.FullVersion + ";1testFramework/0.0.1;2testFramework/0.0.2",
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			require.Equal(t, tt.versionHeader, versionHeader(tt.buildInfo))
		})
	}
}

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
			WithBuildInfo("database/sql", "1.0.0"),
			WithBuildInfo("my-framework", "2.0.0"),
		)

		ctx, err := m.Context(context.Background())
		require.NoError(t, err)

		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)

		headerValues := md.Get(HeaderVersion)
		require.Len(t, headerValues, 1)
		require.Contains(t, headerValues[0], version.FullVersion)
		require.Contains(t, headerValues[0], "database/sql/1.0.0")
		require.Contains(t, headerValues[0], "my-framework/2.0.0")
	})

	t.Run("BuildInfoAppendBuildInfo", func(t *testing.T) {
		m := New(
			"database",
			nil,
			&trace.Driver{},
			WithBuildInfo("database/sql", "1.2.3"),
		)
		m.AppendBuildInfo("database/sql", "1.2.3")

		ctx, err := m.Context(context.Background())
		require.NoError(t, err)

		md, has := metadata.FromOutgoingContext(ctx)
		require.True(t, has)
		require.Equal(t, []string{version.FullVersion + ";database/sql/1.2.3"}, md.Get(HeaderVersion))
	})
}
