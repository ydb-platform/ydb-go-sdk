package xsql

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockDriver implements driver.Driver for testing
type mockDriver struct{}

func (m *mockDriver) Open(name string) (driver.Conn, error) {
	return nil, driver.ErrSkip
}

func TestUnwrap_DB(t *testing.T) {
	t.Run("CorrectConnector", func(t *testing.T) {
		connector := &Connector{
			done: make(chan struct{}),
		}
		db := sql.OpenDB(connector)
		defer db.Close()

		result, err := Unwrap(db)
		require.NoError(t, err)
		require.Equal(t, connector, result)
	})

	t.Run("WrongDriver", func(t *testing.T) {
		// Create a DB with a mock driver that is not a Connector
		sql.Register("mock_driver_unwrap", &mockDriver{})
		db, err := sql.Open("mock_driver_unwrap", "")
		require.NoError(t, err)
		defer db.Close()

		result, err := Unwrap(db)
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "is not a *driverWrapper")
	})
}
