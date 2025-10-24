package xsql

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestConn_ID(t *testing.T) {
	conn := &Conn{
		cc: &mockCommonConn{id: "test-conn-123"},
	}
	result := conn.ID()
	require.Equal(t, "test-conn-123", result)
}

func TestConn_NodeID(t *testing.T) {
	conn := &Conn{
		cc: &mockCommonConn{nodeID: 42},
	}
	result := conn.NodeID()
	require.Equal(t, uint32(42), result)
}

func TestConn_CheckNamedValue(t *testing.T) {
	conn := &Conn{
		connector: &Connector{
			trace: &trace.DatabaseSQL{},
		},
		ctx: context.Background(),
	}

	// CheckNamedValue should accept all values
	err := conn.CheckNamedValue(&driver.NamedValue{
		Name:  "test",
		Value: "value",
	})
	require.NoError(t, err)

	err = conn.CheckNamedValue(&driver.NamedValue{
		Name:  "number",
		Value: 123,
	})
	require.NoError(t, err)
}

func TestConn_Prepare(t *testing.T) {
	conn := &Conn{}
	stmt, err := conn.Prepare("SELECT 1")
	require.Nil(t, stmt)
	require.ErrorIs(t, err, errDeprecated)
	require.ErrorIs(t, err, driver.ErrSkip)
}

func TestConn_Begin(t *testing.T) {
	t.Run("WithoutTx", func(t *testing.T) {
		conn := &Conn{
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
		}
		tx, err := conn.Begin()
		require.Nil(t, tx)
		require.Error(t, err)
		require.ErrorIs(t, err, errDeprecated)
	})

	t.Run("WithExistingTx", func(t *testing.T) {
		conn := &Conn{
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
			currentTx: &Tx{
				tx: &mockTx{id: "existing-tx"},
			},
		}
		tx, err := conn.Begin()
		require.Nil(t, tx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already has an open")
	})
}

func TestConn_Ping(t *testing.T) {
	conn := &Conn{
		cc: &mockCommonConn{},
		connector: &Connector{
			trace: &trace.DatabaseSQL{},
		},
		ctx: context.Background(),
	}
	err := conn.Ping(context.Background())
	require.NoError(t, err)
}

func TestConn_Close(t *testing.T) {
	conn := &Conn{
		cc: &mockCommonConn{},
		connector: &Connector{
			trace: &trace.DatabaseSQL{},
		},
		ctx: context.Background(),
	}
	err := conn.Close()
	require.NoError(t, err)
}

func TestConn_PrepareContext(t *testing.T) {
	t.Run("ValidConn", func(t *testing.T) {
		conn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
		}
		stmt, err := conn.PrepareContext(context.Background(), "SELECT 1")
		require.NoError(t, err)
		require.NotNil(t, stmt)
	})

	t.Run("InvalidConn", func(t *testing.T) {
		conn := &Conn{
			cc: &mockInvalidConn{},
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
		}
		stmt, err := conn.PrepareContext(context.Background(), "SELECT 1")
		require.Error(t, err)
		require.Nil(t, stmt)
	})
}

func TestConn_BeginTx(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		conn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
		}
		tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.NotNil(t, conn.currentTx)
	})

	t.Run("WithExistingTx", func(t *testing.T) {
		conn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace: &trace.DatabaseSQL{},
			},
			ctx: context.Background(),
			currentTx: &Tx{
				tx: &mockTx{id: "existing"},
			},
		}
		tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
		require.Error(t, err)
		require.Nil(t, tx)
		require.Contains(t, err.Error(), "already has an open")
	})
}

func TestConn_QueryContext(t *testing.T) {
	t.Run("WithoutTx", func(t *testing.T) {
		conn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace:     &trace.DatabaseSQL{},
				bindings:  newMockBindings(),
				clock:     clockwork.NewRealClock(),
				processor: QUERY,
			},
			ctx:       context.Background(),
			lastUsage: xsync.NewLastUsage(),
		}
		rows, err := conn.QueryContext(context.Background(), "SELECT 1", []driver.NamedValue{})
		require.NoError(t, err)
		require.Nil(t, rows) // mockCommonConn returns nil
	})

	t.Run("WithTx", func(t *testing.T) {
		conn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace:     &trace.DatabaseSQL{},
				bindings:  newMockBindings(),
				clock:     clockwork.NewRealClock(),
				processor: QUERY,
			},
			ctx:       context.Background(),
			lastUsage: xsync.NewLastUsage(),
			currentTx: &Tx{
				tx: &mockTx{id: "test-tx"},
			},
		}
		rows, err := conn.QueryContext(context.Background(), "SELECT 1", []driver.NamedValue{})
		require.NoError(t, err)
		require.Nil(t, rows) // mockTx returns nil
	})
}

func TestConn_ExecContext(t *testing.T) {
	t.Run("WithoutTx", func(t *testing.T) {
		conn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace:     &trace.DatabaseSQL{},
				bindings:  newMockBindings(),
				clock:     clockwork.NewRealClock(),
				processor: QUERY,
			},
			ctx:       context.Background(),
			lastUsage: xsync.NewLastUsage(),
		}
		result, err := conn.ExecContext(context.Background(), "INSERT INTO test VALUES (1)", []driver.NamedValue{})
		require.NoError(t, err)
		require.Nil(t, result) // mockCommonConn returns nil
	})

	t.Run("WithTx", func(t *testing.T) {
		conn := &Conn{
			cc: &mockCommonConn{},
			connector: &Connector{
				trace:     &trace.DatabaseSQL{},
				bindings:  newMockBindings(),
				clock:     clockwork.NewRealClock(),
				processor: QUERY,
			},
			ctx:       context.Background(),
			lastUsage: xsync.NewLastUsage(),
			currentTx: &Tx{
				tx: &mockTx{id: "test-tx"},
			},
		}
		result, err := conn.ExecContext(context.Background(), "INSERT INTO test VALUES (1)", []driver.NamedValue{})
		require.NoError(t, err)
		require.Nil(t, result) // mockTx returns nil
	})
}
