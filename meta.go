package ydb

import (
	"context"
	"sync"

	"google.golang.org/grpc/metadata"
)

const (
	metaDatabase = "x-ydb-database"
	metaTicket   = "x-ydb-auth-ticket"
)

type meta struct {
	trace       DriverTrace
	credentials Credentials
	database    string

	once  sync.Once
	mu    sync.RWMutex
	token string
	curr  metadata.MD
}

func (m *meta) init() {
	m.once.Do(func() {
		md := make(metadata.MD, 1)
		md.Set(metaDatabase, m.database)
		m.curr = md
	})
}

func (m *meta) md(ctx context.Context) (md metadata.MD, _ error) {
	m.init()

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
		md := make(metadata.MD, 1)
		md.Set(metaDatabase, m.database)
		return md, nil

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

	m.curr = make(metadata.MD, 2)
	m.curr.Set(metaDatabase, m.database)
	m.curr.Set(metaTicket, m.token)

	return m.curr, nil
}
