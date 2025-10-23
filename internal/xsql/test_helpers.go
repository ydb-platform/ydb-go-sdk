package xsql

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
)

// Shared mock implementations for tests

// mockCommonConn implements common.Conn for testing
type mockCommonConn struct {
	id     string
	nodeID uint32
}

func (m *mockCommonConn) ID() string {
	return m.id
}

func (m *mockCommonConn) NodeID() uint32 {
	return m.nodeID
}

func (m *mockCommonConn) IsValid() bool {
	return true
}

func (m *mockCommonConn) Ping(ctx context.Context) error {
	return nil
}

func (m *mockCommonConn) BeginTx(ctx context.Context, opts driver.TxOptions) (common.Tx, error) {
	return &mockTx{id: "mock-tx"}, nil
}

func (m *mockCommonConn) Close() error {
	return nil
}

func (m *mockCommonConn) Query(ctx context.Context, sql string, p *params.Params) (driver.RowsNextResultSet, error) {
	return nil, nil
}

func (m *mockCommonConn) Exec(ctx context.Context, sql string, p *params.Params) (driver.Result, error) {
	return nil, nil
}

func (m *mockCommonConn) Explain(ctx context.Context, sql string, p *params.Params) (string, string, error) {
	return "", "", nil
}

// mockTx implements common.Tx for testing
type mockTx struct {
	id string
}

func (m *mockTx) ID() string {
	return m.id
}

func (m *mockTx) Commit(ctx context.Context) error {
	return nil
}

func (m *mockTx) Rollback(ctx context.Context) error {
	return nil
}

func (m *mockTx) Exec(ctx context.Context, sql string, p *params.Params) (driver.Result, error) {
	return nil, nil
}

func (m *mockTx) Query(ctx context.Context, sql string, p *params.Params) (driver.RowsNextResultSet, error) {
	return nil, nil
}

// mockInvalidConn is a mock that returns IsValid() = false
type mockInvalidConn struct {
	mockCommonConn
}

func (m *mockInvalidConn) IsValid() bool {
	return false
}

// mockBindings just uses bind.NumericArgs for simple testing
type mockBindings = bind.Bindings

func newMockBindings() mockBindings {
	return bind.Bindings{bind.NumericArgs{}}
}

// mockProcessor for testing Stmt methods
type mockProcessor struct{}

func (m *mockProcessor) Exec(ctx context.Context, sql string, p *params.Params) (driver.Result, error) {
	return nil, nil
}

func (m *mockProcessor) Query(ctx context.Context, sql string, p *params.Params) (driver.RowsNextResultSet, error) {
	return nil, nil
}
