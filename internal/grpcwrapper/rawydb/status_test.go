package rawydb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusCode_Equals(t *testing.T) {
	t.Run("IdenticalStatusCodes", func(t *testing.T) {
		status1 := StatusSuccess
		status2 := StatusSuccess
		require.True(t, status1.Equals(status2))
		require.True(t, status2.Equals(status1)) // symmetric
	})

	t.Run("DifferentStatusCodes", func(t *testing.T) {
		status1 := StatusSuccess
		status2 := StatusInternalError
		require.False(t, status1.Equals(status2))
		require.False(t, status2.Equals(status1)) // symmetric
	})

	t.Run("CustomStatusCodes", func(t *testing.T) {
		status1 := StatusCode(100)
		status2 := StatusCode(100)
		status3 := StatusCode(200)

		require.True(t, status1.Equals(status2))
		require.False(t, status1.Equals(status3))
	})

	t.Run("ZeroValue", func(t *testing.T) {
		var status1, status2 StatusCode
		require.True(t, status1.Equals(status2))

		status1 = StatusSuccess
		require.False(t, status1.Equals(status2))
	})
}
