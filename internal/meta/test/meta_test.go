package test

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
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

	testutil.Equal(t, []string{"database"}, md.Get(meta.HeaderDatabase))
	testutil.Equal(t, []string{"requestType"}, md.Get(meta.HeaderRequestType))
	testutil.Equal(t, []string{"token"}, md.Get(meta.HeaderTicket))
	testutil.Equal(t, []string{"userAgent", "user-agent"}, md.Get(meta.HeaderUserAgent))
	testutil.Equal(t, []string{"traceID"}, md.Get(meta.HeaderTraceID))
	testutil.Equal(t, []string{
		"ydb-go-sdk/" + meta.VersionMajor + "." + meta.VersionMinor + "." + meta.VersionPatch,
	}, md.Get(meta.HeaderVersion))
	testutil.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
