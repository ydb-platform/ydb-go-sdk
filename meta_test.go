package ydb

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"google.golang.org/grpc/metadata"
	"testing"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := &meta{
		database:     "database",
		requestsType: "requestType",
		credentials: CredentialsFunc(func(context.Context) (string, error) {
			return "token", nil
		}),
	}

	ctx := context.Background()

	ctx = WithUserAgent(ctx, "userAgent")

	ctx = WithTraceID(ctx, "traceID")

	ctx = metadata.AppendToOutgoingContext(ctx, "some-user-header", "some-user-value")

	md, err := m.md(ctx)
	if err != nil {
		t.Fatal(err)
	}

	internal.Equal(t, []string{"database"}, md.Get(metaDatabase))
	internal.Equal(t, []string{"requestType"}, md.Get(metaRequestType))
	internal.Equal(t, []string{"token"}, md.Get(metaTicket))
	internal.Equal(t, []string{"userAgent"}, md.Get(metaUserAgent))
	internal.Equal(t, []string{"traceID"}, md.Get(metaTraceID))
	internal.Equal(t, []string{Version}, md.Get(metaVersion))
	internal.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
