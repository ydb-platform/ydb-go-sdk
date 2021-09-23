package meta

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"

	"google.golang.org/grpc/metadata"
)

func TestMetaRequiredHeaders(t *testing.T) {
	m := &meta{
		database:     "database",
		requestsType: "requestType",
		credentials: credentials.CredentialsFunc(func(context.Context) (string, error) {
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

	assert.Equal(t, []string{"database"}, md.Get(metaDatabase))
	assert.Equal(t, []string{"requestType"}, md.Get(metaRequestType))
	assert.Equal(t, []string{"token"}, md.Get(metaTicket))
	assert.Equal(t, []string{"userAgent"}, md.Get(metaUserAgent))
	assert.Equal(t, []string{"traceID"}, md.Get(metaTraceID))
	assert.Equal(t, []string{Version}, md.Get(metaVersion))
	assert.Equal(t, []string{"some-user-value"}, md.Get("some-user-header"))
}
