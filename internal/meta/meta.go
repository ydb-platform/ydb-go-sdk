package meta

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	// outgoing headers
	HeaderDatabase           = "x-ydb-database"
	HeaderTicket             = "x-ydb-auth-ticket"
	HeaderVersion            = "x-ydb-sdk-build-info"
	HeaderRequestType        = "x-ydb-request-type"
	HeaderTraceID            = "x-ydb-trace-id"
	HeaderUserAgent          = "x-ydb-user-agent"
	HeaderClientCapabilities = "x-ydb-client-capabilities"

	// outgoing hints
	HintSessionBalancer = "session-balancer"

	// incomming headers
	HeaderServerHints = "x-ydb-server-hints"

	// incoming hints
	HintSessionClose = "session-close"
)

type Meta interface {
	Context(ctx context.Context) (context.Context, error)
}

func New(
	database string,
	credentials credentials.Credentials,
	trace trace.Driver,
	opts ...Option,
) Meta {
	m := &meta{
		trace:       trace,
		credentials: credentials,
		database:    database,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

type Option func(m *meta)

func WithUserAgentOption(userAgent string) Option {
	return func(m *meta) {
		m.userAgents = append(m.userAgents, userAgent)
	}
}

func WithRequestTypeOption(requestType string) Option {
	return func(m *meta) {
		m.requestsType = requestType
	}
}

func AllowOption(feature string) Option {
	return func(m *meta) {
		m.capabilities = append(m.capabilities, feature)
	}
}

func ForbidOption(feature string) Option {
	return func(m *meta) {
		n := 0
		for _, capability := range m.capabilities {
			if capability != feature {
				m.capabilities[n] = capability
				n++
			}
		}
		m.capabilities = m.capabilities[:n]
	}
}

type meta struct {
	trace        trace.Driver
	credentials  credentials.Credentials
	database     string
	requestsType string
	userAgents   []string
	capabilities []string
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
		md.Set(HeaderVersion, "ydb-go-sdk/"+Version)
	}

	if m.requestsType != "" {
		if len(md.Get(HeaderRequestType)) == 0 {
			md.Set(HeaderRequestType, m.requestsType)
		}
	}

	if len(m.userAgents) != 0 {
		md.Append(HeaderUserAgent, m.userAgents...)
	}

	if len(m.capabilities) > 0 {
		md.Append(HeaderClientCapabilities, m.capabilities...)
	}

	if m.credentials == nil {
		return md, nil
	}

	var token string

	done := trace.DriverOnGetCredentials(m.trace, &ctx)
	defer func() {
		done(token, err)
	}()

	token, err = m.credentials.Token(ctx)
	if err != nil {
		if stringer, ok := m.credentials.(fmt.Stringer); ok {
			return nil, xerrors.WithStackTrace(fmt.Errorf("%w: %s", err, stringer.String()))
		}
		return nil, xerrors.WithStackTrace(err)
	}

	md.Set(HeaderTicket, token)

	return md, nil
}

func (m *meta) Context(ctx context.Context) (_ context.Context, err error) {
	md, err := m.meta(ctx)
	if err != nil {
		return ctx, xerrors.WithStackTrace(err)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}
