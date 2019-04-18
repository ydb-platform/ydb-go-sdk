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
	credentials Credentials

	mu       sync.RWMutex
	database string
	token    string
	curr     metadata.MD
}

func newMeta(database string, c Credentials) *meta {
	md := make(metadata.MD, 1)
	md.Set(metaDatabase, database)
	return &meta{
		credentials: c,
		database:    database,
		curr:        md,
	}
}

func (m *meta) md(ctx context.Context) (md metadata.MD, err error) {
	if m.credentials == nil {
		return m.curr, nil
	}
	token, err := m.credentials.Token(ctx)
	if err != nil {
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
