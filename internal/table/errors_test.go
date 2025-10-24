package table

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestErrors(t *testing.T) {
	t.Run("errNilClient", func(t *testing.T) {
		require.NotNil(t, errNilClient)
		require.Contains(t, errNilClient.Error(), "table client is not initialized")
	})

	t.Run("errClosedClient", func(t *testing.T) {
		require.NotNil(t, errClosedClient)
		require.Contains(t, errClosedClient.Error(), "table client closed early")
	})

	t.Run("errParamsRequired", func(t *testing.T) {
		require.NotNil(t, errParamsRequired)
		require.Contains(t, errParamsRequired.Error(), "params required")
	})

	t.Run("errors are wrapped", func(t *testing.T) {
		// Verify that errors are wrapped with xerrors
		require.True(t, xerrors.Is(errNilClient, errNilClient))
		require.True(t, xerrors.Is(errClosedClient, errClosedClient))
		require.True(t, xerrors.Is(errParamsRequired, errParamsRequired))
	})

	t.Run("errors are distinct", func(t *testing.T) {
		require.False(t, xerrors.Is(errNilClient, errClosedClient))
		require.False(t, xerrors.Is(errClosedClient, errParamsRequired))
		require.False(t, xerrors.Is(errNilClient, errParamsRequired))
	})
}
