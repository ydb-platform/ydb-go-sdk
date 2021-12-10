package meta

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
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

	testutil.Equal(t, []string{"database"}, md.Get(metaDatabase))
	testutil.Equal(t, []string{"requestType"}, md.Get(metaRequestType))
	testutil.Equal(t, []string{"token"}, md.Get(metaTicket))
	testutil.Equal(t, []string{"userAgent"}, md.Get(metaUserAgent))
	testutil.Equal(t, []string{"traceID"}, md.Get(metaTraceID))
	testutil.Equal(t, []string{Version}, md.Get(metaVersion))
	testutil.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
