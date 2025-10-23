package xsql

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTx_ID(t *testing.T) {
	tx := &Tx{
		tx: &mockTx{id: "test-tx-123"},
	}
	result := tx.ID()
	require.Equal(t, "test-tx-123", result)

	// Test nil case separately
	nilTx := &Tx{tx: nil}
	require.Equal(t, "", nilTx.ID())
}

func TestTx_Commit(t *testing.T) {
	mockConn := &Conn{
		connector: &Connector{
			trace: &trace.DatabaseSQL{},
		},
	}
	tx := &Tx{
		conn: mockConn,
		tx:   &mockTx{id: "test-tx"},
		ctx:  context.Background(),
	}
	mockConn.currentTx = tx

	err := tx.Commit()
	require.NoError(t, err)
	require.Nil(t, mockConn.currentTx)
}

func TestTx_Rollback(t *testing.T) {
	mockConn := &Conn{
		connector: &Connector{
			trace: &trace.DatabaseSQL{},
		},
	}
	tx := &Tx{
		conn: mockConn,
		tx:   &mockTx{id: "test-tx"},
		ctx:  context.Background(),
	}
	mockConn.currentTx = tx

	err := tx.Rollback()
	require.NoError(t, err)
	require.Nil(t, mockConn.currentTx)
}

func TestTx_QueryContext(t *testing.T) {
	mockConn := &Conn{
		cc: &mockCommonConn{},
		connector: &Connector{
			trace:    &trace.DatabaseSQL{},
			bindings: newMockBindings(),
		},
		ctx: context.Background(),
	}
	tx := &Tx{
		conn: mockConn,
		tx:   &mockTx{id: "test-tx"},
		ctx:  context.Background(),
	}

	rows, err := tx.QueryContext(context.Background(), "SELECT 1", []driver.NamedValue{})
	require.NoError(t, err)
	require.Nil(t, rows) // mockTx returns nil
}

func TestTx_ExecContext(t *testing.T) {
	mockConn := &Conn{
		cc: &mockCommonConn{},
		connector: &Connector{
			trace:    &trace.DatabaseSQL{},
			bindings: newMockBindings(),
		},
		ctx: context.Background(),
	}
	tx := &Tx{
		conn: mockConn,
		tx:   &mockTx{id: "test-tx"},
		ctx:  context.Background(),
	}

	result, err := tx.ExecContext(context.Background(), "INSERT INTO test VALUES (1)", []driver.NamedValue{})
	require.NoError(t, err)
	require.Nil(t, result) // mockTx returns nil
}

func TestTx_PrepareContext(t *testing.T) {
	t.Run("ValidConn", func(t *testing.T) {
		mockConn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
		}
		tx := &Tx{
			conn: mockConn,
			tx:   &mockTx{id: "test-tx"},
			ctx:  context.Background(),
		}

		stmt, err := tx.PrepareContext(context.Background(), "SELECT 1")
		require.NoError(t, err)
		require.NotNil(t, stmt)
	})

	t.Run("InvalidConn", func(t *testing.T) {
		mockConn := &Conn{
			cc: &mockInvalidConn{},
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
		}
		tx := &Tx{
			conn: mockConn,
			tx:   &mockTx{id: "test-tx"},
			ctx:  context.Background(),
		}

		stmt, err := tx.PrepareContext(context.Background(), "SELECT 1")
		require.Error(t, err)
		require.Nil(t, stmt)
	})
}
