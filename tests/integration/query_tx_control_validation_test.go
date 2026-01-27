//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryTxControlValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	t.Run("Client", func(t *testing.T) {
		t.Run("Exec", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				err := db.Query().Exec(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						query.CommitTx(),
					)),
				)
				require.NoError(t, err)
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				err := db.Query().Exec(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						// Missing query.CommitTx()
					)),
				)
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})

		t.Run("Query", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				result, err := db.Query().Query(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						query.CommitTx(),
					)),
				)
				require.NoError(t, err)
				defer func() { _ = result.Close(ctx) }()
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				_, err := db.Query().Query(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						// Missing query.CommitTx()
					)),
				)
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})

		t.Run("QueryResultSet", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				rs, err := db.Query().QueryResultSet(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						query.CommitTx(),
					)),
				)
				require.NoError(t, err)
				defer func() { _ = rs.Close(ctx) }()
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				_, err := db.Query().QueryResultSet(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						// Missing query.CommitTx()
					)),
				)
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})

		t.Run("QueryRow", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				_, err := db.Query().QueryRow(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						query.CommitTx(),
					)),
				)
				require.NoError(t, err)
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				_, err := db.Query().QueryRow(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSerializableReadWrite()),
						// Missing query.CommitTx()
					)),
				)
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})
	})

	t.Run("Session", func(t *testing.T) {
		t.Run("Exec", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					return s.Exec(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							query.CommitTx(),
						)),
					)
				})
				require.NoError(t, err)
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					return s.Exec(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							// Missing query.CommitTx()
						)),
					)
				})
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})

		t.Run("Query", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					result, err := s.Query(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							query.CommitTx(),
						)),
					)
					if err != nil {
						return err
					}
					defer func() { _ = result.Close(ctx) }()
					return nil
				})
				require.NoError(t, err)
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					_, err := s.Query(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							// Missing query.CommitTx()
						)),
					)
					return err
				})
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})

		t.Run("QueryResultSet", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					rs, err := s.QueryResultSet(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							query.CommitTx(),
						)),
					)
					if err != nil {
						return err
					}
					defer func() { _ = rs.Close(ctx) }()
					return nil
				})
				require.NoError(t, err)
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					_, err := s.QueryResultSet(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							// Missing query.CommitTx()
						)),
					)
					return err
				})
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})

		t.Run("QueryRow", func(t *testing.T) {
			t.Run("HappyWay", func(t *testing.T) {
				// With CommitTx - should succeed
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					_, err := s.QueryRow(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							query.CommitTx(),
						)),
					)
					return err
				})
				require.NoError(t, err)
			})

			t.Run("WithoutCommit", func(t *testing.T) {
				// Without CommitTx - should fail with validation error
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					_, err := s.QueryRow(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSerializableReadWrite()),
							// Missing query.CommitTx()
						)),
					)
					return err
				})
				require.Error(t, err)
				require.ErrorIs(t, err, query.ErrTxControlWithoutCommit)
			})
		})
	})

	t.Run("ReadOnlyTransactions", func(t *testing.T) {
		t.Run("Client", func(t *testing.T) {
			t.Run("SnapshotReadOnly", func(t *testing.T) {
				// Read-only transactions should work without CommitTx
				result, err := db.Query().Query(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithSnapshotReadOnly()),
						// No CommitTx needed for read-only
					)),
				)
				require.NoError(t, err)
				defer func() { _ = result.Close(ctx) }()
			})

			t.Run("StaleReadOnly", func(t *testing.T) {
				// Read-only transactions should work without CommitTx
				result, err := db.Query().Query(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithStaleReadOnly()),
						// No CommitTx needed for read-only
					)),
				)
				require.NoError(t, err)
				defer func() { _ = result.Close(ctx) }()
			})

			t.Run("OnlineReadOnly", func(t *testing.T) {
				// Read-only transactions should work without CommitTx
				result, err := db.Query().Query(ctx, "SELECT 1",
					query.WithTxControl(query.TxControl(
						query.BeginTx(query.WithOnlineReadOnly()),
						// No CommitTx needed for read-only
					)),
				)
				require.NoError(t, err)
				defer func() { _ = result.Close(ctx) }()
			})
		})

		t.Run("Session", func(t *testing.T) {
			t.Run("SnapshotReadOnly", func(t *testing.T) {
				// Read-only transactions should work without CommitTx
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					result, err := s.Query(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithSnapshotReadOnly()),
							// No CommitTx needed for read-only
						)),
					)
					if err != nil {
						return err
					}
					defer func() { _ = result.Close(ctx) }()
					return nil
				})
				require.NoError(t, err)
			})

			t.Run("StaleReadOnly", func(t *testing.T) {
				// Read-only transactions should work without CommitTx
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					result, err := s.Query(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithStaleReadOnly()),
							// No CommitTx needed for read-only
						)),
					)
					if err != nil {
						return err
					}
					defer func() { _ = result.Close(ctx) }()
					return nil
				})
				require.NoError(t, err)
			})

			t.Run("OnlineReadOnly", func(t *testing.T) {
				// Read-only transactions should work without CommitTx
				err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
					result, err := s.Query(ctx, "SELECT 1",
						query.WithTxControl(query.TxControl(
							query.BeginTx(query.WithOnlineReadOnly()),
							// No CommitTx needed for read-only
						)),
					)
					if err != nil {
						return err
					}
					defer func() { _ = result.Close(ctx) }()
					return nil
				})
				require.NoError(t, err)
			})
		})
	})
}
