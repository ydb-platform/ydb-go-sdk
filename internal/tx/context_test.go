package tx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithCommitTx(t *testing.T) {
	t.Run("NotSet", func(t *testing.T) {
		ctx := context.Background()
		require.False(t, CommitTxFromContext(ctx))
	})

	t.Run("Set", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithCommitTx(ctx)
		require.True(t, CommitTxFromContext(ctx))
	})
}
