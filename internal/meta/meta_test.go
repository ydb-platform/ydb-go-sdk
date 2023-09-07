package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := New(
		"database",
		credentials.NewAccessTokenCredentials("token"),
		&trace.Driver{},
		WithRequestTypeOption("requestType"),
		WithUserAgentOption("user-agent"),
	)

	ctx := context.Background()

	ctx = meta.WithUserAgent(ctx, "userAgent")

	ctx = meta.WithTraceID(ctx, "traceID")

	ctx = metadata.AppendToOutgoingContext(ctx, "some-user-header", "some-user-value")

	ctx, err := m.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		t.Fatal("no outgoing metadata")
	}

	require.Equal(t, []string{"database"}, md.Get(HeaderDatabase))
	require.Equal(t, []string{"requestType"}, md.Get(HeaderRequestType))
	require.Equal(t, []string{"token"}, md.Get(HeaderTicket))
	require.Equal(t, []string{"userAgent", "user-agent"}, md.Get(HeaderUserAgent))
	require.Equal(t, []string{"traceID"}, md.Get(HeaderTraceID))
	require.Equal(t, []string{
		"ydb-go-sdk/" + version.Major + "." + version.Minor + "." + version.Patch,
	}, md.Get(HeaderVersion))
	require.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
