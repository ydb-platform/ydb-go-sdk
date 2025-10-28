package xsql

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
)

func TestErrorConstants(t *testing.T) {
	t.Run("ErrUnsupported", func(t *testing.T) {
		require.ErrorIs(t, ErrUnsupported, driver.ErrSkip)
	})

	t.Run("errDeprecated", func(t *testing.T) {
		require.ErrorIs(t, errDeprecated, driver.ErrSkip)
	})

	t.Run("errWrongQueryProcessor", func(t *testing.T) {
		require.Error(t, errWrongQueryProcessor)
		require.Equal(t, "wrong query processor", errWrongQueryProcessor.Error())
	})

	t.Run("errNotReadyConn", func(t *testing.T) {
		require.Error(t, errNotReadyConn)
		require.ErrorIs(t, errNotReadyConn, driver.ErrBadConn)
		var badConnErr *badconn.Error
		require.True(t, errors.As(errNotReadyConn, &badConnErr))
	})
}
