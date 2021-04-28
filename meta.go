package ydb

import (
	"context"
	"sync"

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

var (
	userAgentInfo []string
)

// AppendUserAgentInfo appends info to x-ydb-user-agent header
func AppendUserAgentInfo(info ...string) {
	if len(info) == 0 {
		return
	}
	userAgentInfo = append(userAgentInfo, info...)
}

type meta struct {
	trace        DriverTrace
	credentials  Credentials
	database     string
	requestsType string

	once  sync.Once
	mu    sync.RWMutex
	token string
	curr  metadata.MD
}

func (m *meta) make() metadata.MD {
	newMeta := metadata.New(map[string]string{
		metaDatabase: m.database,
		metaVersion:  Version,
	})
	newMeta[metaUserAgent] = userAgentInfo
	if m.requestsType != "" {
		newMeta.Set(metaRequestType, m.requestsType)
	}
	if m.token != "" {
		newMeta.Set(metaTicket, m.token)
	}
	return newMeta
}

func (m *meta) md(ctx context.Context) (md metadata.MD, _ error) {
	m.once.Do(func() {
		m.curr = m.make()
	})

	if m.credentials == nil {
		return m.curr, nil
	}

	m.trace.getCredentialsStart(ctx)
	token, err := m.credentials.Token(ctx)
	defer func() {
		_, withToken := md[metaTicket]
		m.trace.getCredentialsDone(ctx, withToken, err)
	}()

	switch err {
	case nil:
		// Continue.

	case ErrCredentialsDropToken:
		m.mu.Lock()
		defer m.mu.Unlock()
		m.token = ""
		return m.make(), nil

	case ErrCredentialsKeepToken:
		return m.curr, nil

	default:
		return nil, err
	}

	m.mu.RLock()
	changed := m.token != token
	md = m.curr
	m.mu.RUnlock()
	if !changed {
		return md, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.token == token {
		return m.curr, nil
	}
	m.token = token

	m.curr = m.make()
	return m.curr, nil
}
