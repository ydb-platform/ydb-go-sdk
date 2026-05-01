package xquery

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// txFakeCore implements internalQuery.Core for use in tests only.
type txFakeCore struct {
	id     string
	status internalQuery.Status
	done   chan struct{}
}

func (f *txFakeCore) NodeID() uint32 { return 0 }

func (f *txFakeCore) ID() string { return f.id }

func (f *txFakeCore) Status() string { return f.status.String() }

func (f *txFakeCore) Close(_ context.Context) error { return nil }

func (f *txFakeCore) IsAlive() bool { return internalQuery.IsAlive(f.status) }

func (f *txFakeCore) Done() <-chan struct{} { return f.done }

func (f *txFakeCore) SetStatus(s internalQuery.Status) { f.status = s }

// txFakeRollbackNil implements query.Transaction with Rollback returning nil.
// Embedding baseTx.LazyID satisfies the tx.Identifier interface (which has an
// unexported method and therefore cannot be implemented from outside its package).
type txFakeRollbackNil struct {
	baseTx.LazyID
}

func (f *txFakeRollbackNil) Exec(_ context.Context, _ string, _ ...query.ExecuteOption) error {
	panic("txFakeRollbackNil.Exec not implemented in test")
}

func (f *txFakeRollbackNil) Query(_ context.Context, _ string, _ ...query.ExecuteOption) (query.Result, error) {
	panic("txFakeRollbackNil.Query not implemented in test")
}

func (f *txFakeRollbackNil) QueryResultSet(
	_ context.Context, _ string, _ ...query.ExecuteOption,
) (query.ClosableResultSet, error) {
	panic("txFakeRollbackNil.QueryResultSet not implemented in test")
}

func (f *txFakeRollbackNil) QueryRow(_ context.Context, _ string, _ ...query.ExecuteOption) (query.Row, error) {
	panic("txFakeRollbackNil.QueryRow not implemented in test")
}

func (f *txFakeRollbackNil) CommitTx(_ context.Context) error {
	panic("txFakeRollbackNil.CommitTx not implemented in test")
}

func (f *txFakeRollbackNil) Rollback(_ context.Context) error {
	return nil
}

// TestTransactionRollbackReturnsErrBadConnWhenSessionInvalid checks that a
// nil-error rollback still returns driver.ErrBadConn when the underlying
// session is no longer valid (e.g. was invalidated by a BAD_SESSION error
// during the transaction). This ensures database/sql discards the connection
// instead of returning a dead session to the pool.
func TestTransactionRollbackReturnsErrBadConnWhenSessionInvalid(t *testing.T) {
	ctx := xtest.Context(t)

	core := &txFakeCore{
		id:     "dead-session",
		status: internalQuery.StatusClosed, // session is invalid after BAD_SESSION
		done:   make(chan struct{}),
	}
	session := &internalQuery.Session{Core: core}
	conn := New(ctx, session)

	tx := &transaction{
		conn: conn,
		tx:   &txFakeRollbackNil{},
	}

	err := tx.Rollback(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, driver.ErrBadConn,
		"expected driver.ErrBadConn when session is invalid after a nil-error rollback, got: %v", err)
}
