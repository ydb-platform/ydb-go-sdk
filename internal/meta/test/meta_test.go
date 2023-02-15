package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := internal.New(
		"database",
		credentials.NewAccessTokenCredentials("token", "TestMetaRequiredHeaders"),
		trace.Driver{},
		internal.WithRequestTypeOption("requestType"),
		internal.WithUserAgentOption("user-agent"),
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

	require.Equal(t, []string{"database"}, md.Get(internal.HeaderDatabase))
	require.Equal(t, []string{"requestType"}, md.Get(internal.HeaderRequestType))
	require.Equal(t, []string{"token"}, md.Get(internal.HeaderTicket))
	require.Equal(t, []string{"userAgent", "user-agent"}, md.Get(internal.HeaderUserAgent))
	require.Equal(t, []string{"traceID"}, md.Get(internal.HeaderTraceID))
	require.Equal(t, []string{
		"ydb-go-sdk/" + internal.VersionMajor + "." + internal.VersionMinor + "." + internal.VersionPatch,
	}, md.Get(internal.HeaderVersion))
	require.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
