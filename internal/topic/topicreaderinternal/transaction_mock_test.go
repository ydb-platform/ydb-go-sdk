package topicreaderinternal

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

func newMockTransactionWrapper(sessionID, transactinID string) *mockTransaction {
	return &mockTransaction{
		Identifier:     tx.ID("lazy-id"),
		materializedID: tx.ID(transactinID),
		sessionID:      sessionID,
		onCompleted:    nil,
	}
}

type mockTransaction struct {
	tx.Identifier
	materializedID tx.Identifier
	materialized   bool
	sessionID      string
	onCompleted    []tx.OnTransactionCompletedFunc
	RolledBack     bool
}

func (m *mockTransaction) UnLazy(_ context.Context) error {
	m.materialized = true
	m.Identifier = m.materializedID

	return nil
}

func (m *mockTransaction) SessionID() string {
	return m.sessionID
}

func (m *mockTransaction) OnBeforeCommit(f tx.OnTransactionBeforeCommit) {
}

func (m *mockTransaction) OnCompleted(f tx.OnTransactionCompletedFunc) {
	m.onCompleted = append(m.onCompleted, f)
}

func (m *mockTransaction) Rollback(ctx context.Context) error {
	m.RolledBack = true
	for _, f := range m.onCompleted {
		f(query.ErrTransactionRollingBack)
	}

	return nil
}
