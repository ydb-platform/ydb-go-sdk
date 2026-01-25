package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestValidateTxControl(t *testing.T) {
	t.Run("NoUserProvidedTxControl", func(t *testing.T) {
		// Default tx control should not trigger error
		settings := options.ExecuteSettings()
		err := validateTxControl(settings)
		require.NoError(t, err)
	})

	t.Run("UserProvidedTxControlWithCommit", func(t *testing.T) {
		// User provided TxControl with CommitTx should not trigger error
		settings := options.ExecuteSettings(
			options.WithTxControl(
				tx.NewControl(
					tx.BeginTx(tx.WithSerializableReadWrite()),
					tx.CommitTx(),
				),
			),
		)
		err := validateTxControl(settings)
		require.NoError(t, err)
	})

	t.Run("UserProvidedTxControlWithoutCommit", func(t *testing.T) {
		// User provided TxControl without CommitTx should trigger error
		settings := options.ExecuteSettings(
			options.WithTxControl(
				tx.NewControl(
					tx.BeginTx(tx.WithSerializableReadWrite()),
				),
			),
		)
		err := validateTxControl(settings)
		require.Error(t, err)
		require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
	})

	t.Run("UserProvidedTxControlWithTxID", func(t *testing.T) {
		// User provided TxControl with TxID (not BeginTx) should not trigger error
		settings := options.ExecuteSettings(
			options.WithTxControl(
				tx.NewControl(
					tx.WithTxID("some-tx-id"),
				),
			),
		)
		err := validateTxControl(settings)
		require.NoError(t, err)
	})

	t.Run("NilTxControl", func(t *testing.T) {
		// Nil TxControl should not trigger error
		settings := options.ExecuteSettings(
			options.WithTxControl(nil),
		)
		err := validateTxControl(settings)
		require.NoError(t, err)
	})
}

func TestClientExecWithInvalidTxControl(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	client := NewMockQueryServiceClient(ctrl)

	c := testClient(t, client)
	defer func() {
		_ = c.Close(ctx)
	}()

	// Try to exec with TxControl that has BeginTx but no CommitTx
	err := c.Exec(ctx, "INSERT INTO t ...",
		options.WithTxControl(
			tx.NewControl(
				tx.BeginTx(tx.WithSerializableReadWrite()),
			),
		),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
}

func TestClientQueryWithInvalidTxControl(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	client := NewMockQueryServiceClient(ctrl)

	c := testClient(t, client)
	defer func() {
		_ = c.Close(ctx)
	}()

	// Try to query with TxControl that has BeginTx but no CommitTx
	_, err := c.Query(ctx, "SELECT * FROM t",
		options.WithTxControl(
			tx.NewControl(
				tx.BeginTx(tx.WithSerializableReadWrite()),
			),
		),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
}

func TestClientQueryResultSetWithInvalidTxControl(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	client := NewMockQueryServiceClient(ctrl)

	c := testClient(t, client)
	defer func() {
		_ = c.Close(ctx)
	}()

	// Try to query with TxControl that has BeginTx but no CommitTx
	_, err := c.QueryResultSet(ctx, "SELECT * FROM t",
		options.WithTxControl(
			tx.NewControl(
				tx.BeginTx(tx.WithSerializableReadWrite()),
			),
		),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
}

func TestClientQueryRowWithInvalidTxControl(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	client := NewMockQueryServiceClient(ctrl)

	c := testClient(t, client)
	defer func() {
		_ = c.Close(ctx)
	}()

	// Try to query with TxControl that has BeginTx but no CommitTx
	_, err := c.QueryRow(ctx, "SELECT * FROM t",
		options.WithTxControl(
			tx.NewControl(
				tx.BeginTx(tx.WithSerializableReadWrite()),
			),
		),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
}

func testClient(t *testing.T, client *MockQueryServiceClient) *Client {
	return &Client{
		config: config.New(),
		client: client,
		done:   make(chan struct{}),
		explicitSessionPool: &mockSessionPool{
			withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
				return f(ctx, &Session{})
			},
		},
		implicitSessionPool: &mockSessionPool{
			withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
				return f(ctx, &Session{})
			},
		},
	}
}

type mockSessionPool struct {
	withFunc func(ctx context.Context, f func(ctx context.Context, s *Session) error) error
}

func (m *mockSessionPool) Close(ctx context.Context) error {
	return nil
}

func (m *mockSessionPool) With(ctx context.Context,
	f func(ctx context.Context, s *Session) error,
	opts ...retry.Option,
) error {
	if m.withFunc != nil {
		return m.withFunc(ctx, f)
	}

	return nil
}

func (m *mockSessionPool) Stats() pool.Stats {
	return pool.Stats{}
}
