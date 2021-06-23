package ydb

import (
	"context"
	"google.golang.org/grpc/metadata"
)

const (
	metaDatabase    = "x-ydb-database"
	metaTicket      = "x-ydb-auth-ticket"
	metaVersion     = "x-ydb-sdk-build-info"
	metaRequestType = "x-ydb-request-type"
	metaTraceID     = "x-ydb-trace-id"
	metaUserAgent   = "x-ydb-user-agent"
)

type meta struct {
	trace        DriverTrace
	credentials  Credentials
	database     string
	requestsType string
}

func (m *meta) md(ctx context.Context) (md metadata.MD, err error) {
	var has bool
	if md, has = metadata.FromOutgoingContext(ctx); !has {
		md = metadata.MD{}
	}
	md.Set(metaDatabase, m.database)
	md.Set(metaVersion, Version)
	if m.requestsType != "" {
		md.Set(metaRequestType, m.requestsType)
	}
	if m.credentials != nil {
		var token string
		m.trace.getCredentialsStart(ctx)
		defer func() {
			m.trace.getCredentialsDone(ctx, token != "", err)
		}()
		token, err = m.credentials.Token(ctx)
		if err != nil {
			return nil, err
		}
		md.Set(metaTicket, token)
	}
	return md, nil
}
