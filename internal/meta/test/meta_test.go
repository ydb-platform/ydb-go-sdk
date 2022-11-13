package test

import (
	"context"
	meta2 "github.com/ydb-platform/ydb-go-sdk/v3/meta"
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

	ctx = meta2.WithUserAgent(ctx, "userAgent")

	ctx = meta2.WithTraceID(ctx, "traceID")

	ctx = metadata.AppendToOutgoingContext(ctx, "some-user-header", "some-user-value")

	ctx, err := m.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		t.Fatal("no outgoing metadata")
	}

	require.Equal(t, []string{"database"}, md.Get(meta2.HeaderDatabase))
	require.Equal(t, []string{"requestType"}, md.Get(meta2.HeaderRequestType))
	require.Equal(t, []string{"token"}, md.Get(meta2.HeaderTicket))
	require.Equal(t, []string{"userAgent", "user-agent"}, md.Get(meta2.HeaderUserAgent))
	require.Equal(t, []string{"traceID"}, md.Get(meta2.HeaderTraceID))
	require.Equal(t, []string{
		"ydb-go-sdk/" + meta2.VersionMajor + "." + meta2.VersionMinor + "." + meta2.VersionPatch,
	}, md.Get(meta2.HeaderVersion))
	require.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
