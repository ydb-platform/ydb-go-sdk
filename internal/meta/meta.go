package meta

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	// outgoing headers
	HeaderDatabase    = "x-ydb-database"
	HeaderTicket      = "x-ydb-auth-ticket"
	HeaderVersion     = "x-ydb-sdk-build-info"
	HeaderRequestType = "x-ydb-request-type"
	HeaderTraceID     = "x-ydb-trace-id"
	HeaderUserAgent   = "x-ydb-user-agent"

	// incomming headers
	HeaderServerHints = "x-ydb-server-hints"

	// hints
	HintSessionClose = "session-close"
)

type Meta interface {
	Meta(ctx context.Context) (context.Context, error)

	WithDatabase(database string) Meta
	WithCredentials(creds credentials.Credentials) Meta
	WithUserAgent(userAgent string) Meta

	Database() string
	UserAgent() string
}

func New(
	database string,
	credentials credentials.Credentials,
	trace trace.Driver,
	requestsType string,
	userAgent string,
) Meta {
	return &meta{
		trace:        trace,
		credentials:  credentials,
		database:     database,
		requestsType: requestsType,
		userAgent:    userAgent,
	}
}

type meta struct {
	trace        trace.Driver
	credentials  credentials.Credentials
	database     string
	requestsType string
	userAgent    string
}

func (m *meta) Database() string {
	return m.database
}

func (m *meta) UserAgent() string {
	return m.userAgent
}

func (m *meta) WithDatabase(database string) Meta {
	mm := *m
	mm.database = database
	return &mm
}

func (m *meta) WithCredentials(creds credentials.Credentials) Meta {
	mm := *m
	mm.credentials = creds
	return &mm
}

func (m *meta) WithUserAgent(userAgent string) Meta {
	mm := *m
	mm.userAgent = userAgent
	return &mm
}

func (m *meta) meta(ctx context.Context) (_ metadata.MD, err error) {
	md, has := metadata.FromOutgoingContext(ctx)
	if !has {
		md = metadata.MD{}
	}
	if len(md.Get(HeaderDatabase)) == 0 {
		md.Set(HeaderDatabase, m.database)
	}
	if len(md.Get(HeaderVersion)) == 0 {
		md.Set(HeaderVersion, Version)
	}
	if m.requestsType != "" {
		if len(md.Get(HeaderRequestType)) == 0 {
			md.Set(HeaderRequestType, m.requestsType)
		}
	}
	if m.userAgent != "" {
		if len(md.Get(HeaderUserAgent)) == 0 {
			md.Set(HeaderUserAgent, m.userAgent)
		}
	}
	if m.credentials == nil {
		return md, nil
	}
	var token string
	t := trace.ContextDriver(ctx).Compose(m.trace)
	getCredentialsDone := trace.DriverOnGetCredentials(t, &ctx)
	defer func() {
		getCredentialsDone(token, err)
	}()
	token, err = m.credentials.Token(ctx)
	if err != nil {
		if stringer, ok := m.credentials.(fmt.Stringer); ok {
			return nil, errors.WithStackTrace(fmt.Errorf("%w: %s", err, stringer.String()))
		}
		return nil, err
	}
	if len(md.Get(HeaderTicket)) == 0 {
		md.Set(HeaderTicket, token)
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
