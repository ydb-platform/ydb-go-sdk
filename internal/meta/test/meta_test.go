package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := internal.New(
		"database",
		credentials.NewAccessTokenCredentials("token"),
		&trace.Driver{},
		internal.WithRequestTypeOption("requestType"),
		internal.WithApplicationNameOption("test app"),
	)

	ctx := context.Background()

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
	require.NotEmpty(t, md.Get(internal.HeaderClientPid))
	require.NotEmpty(t, md.Get(internal.HeaderClientPid)[0])
	require.Equal(t, []string{"test app"}, md.Get(internal.HeaderApplicationName))
	require.Equal(t, []string{"traceID"}, md.Get(internal.HeaderTraceID))
	require.Equal(t, []string{
		"ydb-go-sdk/" + version.Major + "." + version.Minor + "." + version.Patch,
	}, md.Get(internal.HeaderVersion))
	require.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
