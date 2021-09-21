package meta

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"

	"google.golang.org/grpc/metadata"

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
	trace trace.DriverTrace,
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
	trace        trace.DriverTrace
	credentials  credentials.Credentials
	database     string
	requestsType string
}

func (m *meta) meta(ctx context.Context) (_ metadata.MD, err error) {
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		md = metadata.MD{}
	}
	md.Set(metaDatabase, m.database)
	md.Set(metaVersion, Version)
	if m.requestsType != "" {
		md.Set(metaRequestType, m.requestsType)
	}
	if m.credentials != nil {
		var token string
		t := trace.ContextDriverTrace(ctx).Compose(m.trace)
		if t.OnGetCredentials != nil {
			getCredentialsDone := t.OnGetCredentials(trace.GetCredentialsStartInfo{
				Context: ctx,
			})
			if getCredentialsDone != nil {
				defer func() {
					getCredentialsDone(trace.GetCredentialsDoneInfo{
						TokenOk: token != "",
						Error:   err,
					})
				}()
			}
		}
		token, err = m.credentials.Token(ctx)
		if err != nil {
			if stringer, ok := m.credentials.(fmt.Stringer); ok {
				return nil, fmt.Errorf("%s: %w", stringer.String(), err)
			}
			return nil, err
		}
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
