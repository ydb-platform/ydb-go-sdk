package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrors(t *testing.T) {
	t.Run("errNilClient", func(t *testing.T) {
		require.NotNil(t, errNilClient)
		require.Contains(t, errNilClient.Error(), "table client is not initialized")
	})

	t.Run("ErrTransactionRollingBack", func(t *testing.T) {
		require.NotNil(t, ErrTransactionRollingBack)
		require.Contains(t, ErrTransactionRollingBack.Error(), "the transaction is rolling back")
	})

	t.Run("errWrongNextResultSetIndex", func(t *testing.T) {
		require.NotNil(t, errWrongNextResultSetIndex)
		require.Equal(t, "wrong result set index", errWrongNextResultSetIndex.Error())
	})

	t.Run("errWrongResultSetIndex", func(t *testing.T) {
		require.NotNil(t, errWrongResultSetIndex)
		require.Equal(t, "critical violation of the logic - wrong result set index", errWrongResultSetIndex.Error())
	})

	t.Run("errMoreThanOneRow", func(t *testing.T) {
		require.NotNil(t, errMoreThanOneRow)
		require.Equal(t, "unexpected more than one row in result set", errMoreThanOneRow.Error())
	})

	t.Run("errMoreThanOneResultSet", func(t *testing.T) {
		require.NotNil(t, errMoreThanOneResultSet)
		require.Equal(t, "unexpected more than one result set", errMoreThanOneResultSet.Error())
	})

	t.Run("errNoResultSets", func(t *testing.T) {
		require.NotNil(t, errNoResultSets)
		require.Equal(t, "no result sets", errNoResultSets.Error())
	})

	t.Run("errNilOption", func(t *testing.T) {
		require.NotNil(t, errNilOption)
		require.Equal(t, "nil option", errNilOption.Error())
	})

	t.Run("ErrOptionNotForTxExecute", func(t *testing.T) {
		require.NotNil(t, ErrOptionNotForTxExecute)
		require.Equal(t, "option is not for execute on transaction", ErrOptionNotForTxExecute.Error())
	})

	t.Run("errExecuteOnCompletedTx", func(t *testing.T) {
		require.NotNil(t, errExecuteOnCompletedTx)
		require.Equal(t, "execute on completed transaction", errExecuteOnCompletedTx.Error())
	})

	t.Run("errSessionClosed", func(t *testing.T) {
		require.NotNil(t, errSessionClosed)
		require.Equal(t, "session is closed", errSessionClosed.Error())
	})

	t.Run("ErrorsAreUnique", func(t *testing.T) {
		// Verify that error variables are distinct
		require.False(t, errors.Is(errNilClient, ErrTransactionRollingBack))
		require.False(t, errors.Is(errWrongNextResultSetIndex, errWrongResultSetIndex))
		require.False(t, errors.Is(errMoreThanOneRow, errMoreThanOneResultSet))
		require.False(t, errors.Is(errNoResultSets, errNilOption))
		require.False(t, errors.Is(ErrOptionNotForTxExecute, errExecuteOnCompletedTx))
		require.False(t, errors.Is(errExecuteOnCompletedTx, errSessionClosed))
	})
}
