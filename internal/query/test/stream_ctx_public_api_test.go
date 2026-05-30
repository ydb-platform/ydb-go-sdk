package test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const commitSelectOneQuery = "select 1"

// TestQueryResultCloseDrainsStatsAfterNextResultSetCanceledCtx uses only the public query API.
//
// Regression (v3.127.7+): forwarding per-call ctx cancellation to executeCtx via streamCancel
// and poisoning lastErr prevented Close(background) from draining late stream parts with ExecStats.
//
// User-visible flow:
//  1. tx.Query returns a result; first stream part has no ExecStats (mock: delayed stats).
//  2. res.NextResultSet(canceledCtx) fails with context.Canceled (e.g. deadline/cancel mid-read).
//  3. res.Close(context.Background()) must still drain the stream and invoke the stats callback.
func TestQueryResultCloseDrainsStatsAfterNextResultSetCanceledCtx(t *testing.T) {
	mockSrv := mock.Server(t, mock.WithCommitStatsDelayed())

	ctx := context.Background()
	db, err := ydb.Open(ctx, mockSrv.ConnString(), ydb.WithAnonymousCredentials())
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	var gotStats atomic.Bool

	err = db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		transaction, err := session.Begin(ctx, query.TxSettings(query.WithSerializableReadWrite()))
		if err != nil {
			return err
		}

		res, err := transaction.Query(
			ctx,
			commitSelectOneQuery,
			query.WithCommit(),
			query.WithStatsMode(query.StatsModeBasic, func(s query.Stats) {
				if s == nil {
					return
				}
				if _, ok := s.NextPhase(); ok {
					gotStats.Store(true)
				}
			}),
		)
		if err != nil {
			return err
		}

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = res.NextResultSet(cancelledCtx)
		if err == nil {
			t.Fatal("expected context.Canceled from NextResultSet with cancelled ctx")
		}
		require.ErrorIs(t, err, context.Canceled)

		return res.Close(context.Background())
	})
	require.NoError(t, err)
	require.True(t, gotStats.Load(),
		"Close with fresh ctx must drain ExecStats after NextResultSet failed on cancelled per-call ctx")
}
