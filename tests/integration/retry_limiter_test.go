//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type noQuota struct{}

func (n noQuota) Acquire(ctx context.Context) error {
	return retry.ErrNoQuota
}

func TestRetryLimiter(t *testing.T) {
	ctx := xtest.Context(t)

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDiscoveryInterval(time.Second),
	)
	require.NoError(t, err)

	defer func() {
		// cleanup
		_ = nativeDriver.Close(ctx)
	}()

	c, err := ydb.Connector(nativeDriver)
	require.NoError(t, err)

	defer func() {
		// cleanup
		_ = c.Close()
	}()

	db := sql.OpenDB(c)
	defer func() {
		// cleanup
		_ = db.Close()
	}()

	l := noQuota{}

	t.Run("retry.Retry", func(t *testing.T) {
		err := retry.Retry(ctx, func(ctx context.Context) (err error) {
			return retry.RetryableError(errors.New("custom error"))
		}, retry.WithLimiter(l))
		require.ErrorIs(t, err, retry.ErrNoQuota)
	})
	t.Run("retry.Do", func(t *testing.T) {
		err := retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			return retry.RetryableError(errors.New("custom error"))
		}, retry.WithLimiter(l))
		require.ErrorIs(t, err, retry.ErrNoQuota)
	})
	t.Run("retry.DoTx", func(t *testing.T) {
		err := retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) (err error) {
			return retry.RetryableError(errors.New("custom error"))
		}, retry.WithLimiter(l))
		require.ErrorIs(t, err, retry.ErrNoQuota)
	})
	t.Run("db.Table().Do", func(t *testing.T) {
		err := nativeDriver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			return retry.RetryableError(errors.New("custom error"))
		}, table.WithRetryOptions(retry.WithLimiter(l)))
		require.ErrorIs(t, err, retry.ErrNoQuota)
	})
	t.Run("db.Table().DoTx", func(t *testing.T) {
		err := nativeDriver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			return retry.RetryableError(errors.New("custom error"))
		}, table.WithRetryOptions(retry.WithLimiter(l)))
		require.ErrorIs(t, err, retry.ErrNoQuota)
	})
	if version.Gte(os.Getenv("YDB_VERSION"), "24.1") {
		t.Run("db.Query().Do", func(t *testing.T) {
			err := nativeDriver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
				return retry.RetryableError(errors.New("custom error"))
			}, query.WithRetryOptions(retry.WithLimiter(l)))
			require.ErrorIs(t, err, retry.ErrNoQuota)
		})
		t.Run("db.Query().DoTx", func(t *testing.T) {
			err := nativeDriver.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
				return retry.RetryableError(errors.New("custom error"))
			}, query.WithRetryOptions(retry.WithLimiter(l)))
			require.ErrorIs(t, err, retry.ErrNoQuota)
		})
	}
}
