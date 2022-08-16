package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := meta.New(
		"database",
		credentials.NewAccessTokenCredentials("token", "TestMetaRequiredHeaders"),
		trace.Driver{},
		meta.WithRequestTypeOption("requestType"),
		meta.WithUserAgentOption("user-agent"),
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

	require.Equal(t, []string{"database"}, md.Get(meta.HeaderDatabase))
	require.Equal(t, []string{"requestType"}, md.Get(meta.HeaderRequestType))
	require.Equal(t, []string{"token"}, md.Get(meta.HeaderTicket))
	require.Equal(t, []string{"userAgent", "user-agent"}, md.Get(meta.HeaderUserAgent))
	require.Equal(t, []string{"traceID"}, md.Get(meta.HeaderTraceID))
	require.Equal(t, []string{
		"ydb-go-sdk/" + meta.VersionMajor + "." + meta.VersionMinor + "." + meta.VersionPatch,
	}, md.Get(meta.HeaderVersion))
	require.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
