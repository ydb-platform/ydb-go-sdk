package topicreaderinternal

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
)

type TransactionWrapper struct {
	queryTx *query.Transaction
	mockTx  *mockTransaction
}

func NewFromQueryTransaction(q *query.Transaction) *TransactionWrapper {
	return &TransactionWrapper{queryTx: q}
}

func newMockTransactionWrapper(sessionID, transactinID string) *TransactionWrapper {
	return &TransactionWrapper{
		mockTx: &mockTransaction{
			ID:          transactinID,
			SessionID:   sessionID,
			OnCompleted: nil,
		},
	}
}

type mockTransaction struct {
	ID          string
	SessionID   string
	OnCompleted []query.OnTransactionCompletedFunc
	RolledBack  bool
}

func (t *TransactionWrapper) ID() string {
	switch {
	case t.queryTx != nil:
		return t.queryTx.ID()
	case t.mockTx != nil:
		return t.mockTx.ID
	default:
		panic("ydb: unexpected transaction wrapper state for ID")
	}
}

func (t *TransactionWrapper) SessionID() string {
	switch {
	case t.queryTx != nil:
		return query.GetSessionID(t.queryTx)
	case t.mockTx != nil:
		return t.mockTx.SessionID
	default:
		panic("ydb: unexpected transaction wrapper state for SessionID")
	}
}

func (t *TransactionWrapper) OnTxCompleted(f query.OnTransactionCompletedFunc) {
	switch {
	case t.queryTx != nil:
		query.OnTransactionCompleted(t.queryTx, f)
	case t.mockTx != nil:
		t.mockTx.OnCompleted = append(t.mockTx.OnCompleted, f)
	default:
		panic("ydb: unexpected transaction wrapper state for OnTxCompleted")
	}
}

func (t *TransactionWrapper) Rollback(ctx context.Context) error {
	switch {
	case t.queryTx != nil:
		return t.queryTx.Rollback(ctx)
	case t.mockTx != nil:
		t.mockTx.RolledBack = true
		return nil
	default:
		panic("ydb: unexpected transaction wrapper state for Rollback")
	}
}
