//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func TestSharedSessionPoolAcrossClients(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver(ydb.WithSessionPoolSizeLimit(1))
	var sessionID string
	scope.Require.NoError(db.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		sessionID = s.ID()
		_, res, err := s.Execute(ctx,
			table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx()), "SELECT 1", nil,
		)
		if err != nil {
			return err
		}
		defer res.Close()
		for res.NextResultSet(ctx) {
			for res.NextRow() {
			}
		}

		return res.Err()
	}))
	scope.Require.NoError(db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		scope.Require.Equal(sessionID, s.ID())

		return s.Exec(ctx, "SELECT 1")
	}))
	stats, err := db.SessionPoolStats()
	scope.Require.NoError(err)
	scope.Require.Equal(1, stats.Limit)
	scope.Require.Equal(1, stats.Size)
}
