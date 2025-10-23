package xsql

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStmt_NumInput(t *testing.T) {
	stmt := &Stmt{}
	result := stmt.NumInput()
	require.Equal(t, -1, result)
}

func TestStmt_Exec(t *testing.T) {
	stmt := &Stmt{}
	result, err := stmt.Exec([]driver.Value{})
	require.Nil(t, result)
	require.ErrorIs(t, err, errDeprecated)
	require.ErrorIs(t, err, driver.ErrSkip)
}

func TestStmt_Query(t *testing.T) {
	stmt := &Stmt{}
	result, err := stmt.Query([]driver.Value{})
	require.Nil(t, result)
	require.ErrorIs(t, err, errDeprecated)
	require.ErrorIs(t, err, driver.ErrSkip)
}

func TestStmt_Close(t *testing.T) {
	stmt := &Stmt{
		conn: &Conn{
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
		},
		ctx: context.Background(),
	}
	err := stmt.Close()
	require.NoError(t, err)
}

func TestStmt_QueryContext(t *testing.T) {
	t.Run("ValidConn", func(t *testing.T) {
		stmt := &Stmt{
			conn: &Conn{
				cc: &mockCommonConn{},
				connector: &Connector{
					trace:    &trace.DatabaseSQL{},
					bindings: newMockBindings(),
				},
			},
			processor: &mockProcessor{},
			ctx:       context.Background(),
			sql:       "SELECT 1",
		}

		rows, err := stmt.QueryContext(context.Background(), []driver.NamedValue{})
		require.NoError(t, err)
		require.Nil(t, rows) // mockProcessor returns nil
	})

	t.Run("InvalidConn", func(t *testing.T) {
		stmt := &Stmt{
			conn: &Conn{
				cc: &mockInvalidConn{},
				connector: &Connector{
					trace: &trace.DatabaseSQL{},
				},
			},
			processor: &mockProcessor{},
			ctx:       context.Background(),
			sql:       "SELECT 1",
		}

		rows, err := stmt.QueryContext(context.Background(), []driver.NamedValue{})
		require.Error(t, err)
		require.Nil(t, rows)
	})
}

func TestStmt_ExecContext(t *testing.T) {
	t.Run("ValidConn", func(t *testing.T) {
		stmt := &Stmt{
			conn: &Conn{
				cc: &mockCommonConn{},
				connector: &Connector{
					trace:    &trace.DatabaseSQL{},
					bindings: newMockBindings(),
				},
			},
			processor: &mockProcessor{},
			ctx:       context.Background(),
			sql:       "INSERT INTO test VALUES (1)",
		}

		result, err := stmt.ExecContext(context.Background(), []driver.NamedValue{})
		require.NoError(t, err)
		require.Nil(t, result) // mockProcessor returns nil
	})

	t.Run("InvalidConn", func(t *testing.T) {
		stmt := &Stmt{
			conn: &Conn{
				cc: &mockInvalidConn{},
				connector: &Connector{
					trace: &trace.DatabaseSQL{},
				},
			},
			processor: &mockProcessor{},
			ctx:       context.Background(),
			sql:       "INSERT INTO test VALUES (1)",
		}

		result, err := stmt.ExecContext(context.Background(), []driver.NamedValue{})
		require.Error(t, err)
		require.Nil(t, result)
	})
}
