package meta

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cmp"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := &meta{
		database:     "database",
		requestsType: "requestType",
		credentials: credentials.Func(func(context.Context) (string, error) {
			return "token", nil
		}),
	}

	ctx := context.Background()

	ctx = WithUserAgent(ctx, "userAgent")

	ctx = WithTraceID(ctx, "traceID")

	ctx = metadata.AppendToOutgoingContext(ctx, "some-user-header", "some-user-value")

	md, err := m.meta(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cmp.Equal(t, []string{"database"}, md.Get(metaDatabase))
	cmp.Equal(t, []string{"requestType"}, md.Get(metaRequestType))
	cmp.Equal(t, []string{"token"}, md.Get(metaTicket))
	cmp.Equal(t, []string{"userAgent"}, md.Get(metaUserAgent))
	cmp.Equal(t, []string{"traceID"}, md.Get(metaTraceID))
	cmp.Equal(t, []string{Version}, md.Get(metaVersion))
	cmp.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
