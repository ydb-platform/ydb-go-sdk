package ydb

import (
	"context"
	"sync"

	"google.golang.org/grpc/metadata"
)

const (
	metaDatabase = "x-ydb-database"
	metaTicket   = "x-ydb-auth-ticket"
	metaVersion  = "x-ydb-sdk-build-info"
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

func (m *meta) make() metadata.MD {
	return metadata.New(map[string]string{
		metaDatabase: m.database,
		metaVersion:  Version,
	})
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

	m.curr = make(metadata.MD, 2)
	m.curr.Set(metaDatabase, m.database)
	m.curr.Set(metaTicket, m.token)

	return m.curr, nil
}
