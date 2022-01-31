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
		"requestType",
		"user-agent",
	)

	ctx := context.Background()

	ctx = meta.WithUserAgent(ctx, "userAgent")

	ctx = meta.WithTraceID(ctx, "traceID")

	ctx = metadata.AppendToOutgoingContext(ctx, "some-user-header", "some-user-value")

	ctx, err := m.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		t.Fatal("no outgoing metadata")
	}

	testutil.Equal(t, []string{"database"}, md.Get(meta.MetaDatabase))
	testutil.Equal(t, []string{"requestType"}, md.Get(meta.MetaRequestType))
	testutil.Equal(t, []string{"token"}, md.Get(meta.MetaTicket))
	testutil.Equal(t, []string{"userAgent"}, md.Get(meta.MetaUserAgent))
	testutil.Equal(t, []string{"traceID"}, md.Get(meta.MetaTraceID))
	testutil.Equal(t, []string{meta.Version}, md.Get(meta.MetaVersion))
	testutil.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
