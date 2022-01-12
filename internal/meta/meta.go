package meta

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	metaDatabase    = "x-ydb-database"
	metaTicket      = "x-ydb-auth-ticket"
	metaVersion     = "x-ydb-sdk-build-info"
	metaRequestType = "x-ydb-request-types"
	metaTraceID     = "x-ydb-trace-id"
	metaUserAgent   = "x-ydb-user-agent"
)

type Meta interface {
	Meta(ctx context.Context) (context.Context, error)
}

func New(
	database string,
	credentials credentials.Credentials,
	trace trace.Driver,
	requestsType string,
) Meta {
	return &meta{
		trace:        trace,
		credentials:  credentials,
		database:     database,
		requestsType: requestsType,
	}
}

type meta struct {
	trace        trace.Driver
	credentials  credentials.Credentials
	database     string
	requestsType string
}

func (m *meta) meta(ctx context.Context) (_ metadata.MD, err error) {
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		md = metadata.MD{}
	}
	if len(md.Get(metaDatabase)) == 0 {
		md.Set(metaDatabase, m.database)
	}
	if len(md.Get(metaVersion)) == 0 {
		md.Set(metaVersion, Version)
	}
	if m.requestsType != "" {
		if len(md.Get(metaRequestType)) == 0 {
			md.Set(metaRequestType, m.requestsType)
		}
	}
	if m.credentials == nil {
		return md, nil
	}
	var token string
	t := trace.ContextDriver(ctx).Compose(m.trace)
	getCredentialsDone := trace.DriverOnGetCredentials(t, &ctx)
	defer func() {
		getCredentialsDone(token != "", err)
	}()
	token, err = m.credentials.Token(ctx)
	if err != nil {
		if stringer, ok := m.credentials.(fmt.Stringer); ok {
			return nil, fmt.Errorf("%s: %w", stringer.String(), err)
		}
		return nil, err
	}
	if len(md.Get(metaTicket)) == 0 {
		md.Set(metaTicket, token)
	}
	return md, nil
}

func (m *meta) Meta(ctx context.Context) (_ context.Context, err error) {
	md, err := m.meta(ctx)
	if err != nil {
		return ctx, err
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}
