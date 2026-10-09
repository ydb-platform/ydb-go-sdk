//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryStrictSerializableCommitTimestamp(t *testing.T) {
	if os.Getenv("YDB_STRICT_SERIALIZABLE_ENABLED") != "1" {
		t.Skip("requires YDB TableServiceConfig.EnableStrictSerializableIsolation")
	}

	scope := newScope(t)
	db := scope.Driver(ydb.WithBalancer(balancers.SingleConn()))
	ctx, cancel := context.WithTimeout(scope.Ctx, 2*time.Minute)
	defer cancel()
	tablePath := db.Name() + "/ssrw_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, db.Query().Exec(ctx,
		fmt.Sprintf("CREATE TABLE `%s` (id Int64 NOT NULL, val Text, PRIMARY KEY (id))", tablePath),
	))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = db.Query().Exec(cleanupCtx, fmt.Sprintf("DROP TABLE `%s`", tablePath))
	})

	var explicitTx query.Transaction
	err := db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		tx, err := session.Begin(ctx, query.TxSettings(query.WithStrictSerializableReadWrite()))
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		explicitTx = tx
		if err := tx.Exec(ctx, fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (1, \"explicit\")", tablePath)); err != nil {
			return err
		}
		if timestamp := tx.(query.CommitTimestampProvider).CommitTimestamp(); timestamp != nil {
			return fmt.Errorf("commit timestamp appeared before CommitTx: %v", timestamp)
		}

		return tx.CommitTx(ctx)
	})
	require.NoError(t, err)
	explicitTimestamp := explicitTx.(query.CommitTimestampProvider).CommitTimestamp()
	require.NotNil(t, explicitTimestamp)
	require.Positive(t, explicitTimestamp.PlanStep())
	require.Positive(t, explicitTimestamp.TxID())
	require.Equal(t, db.Name(), explicitTimestamp.Database())

	var executeTimestamp *query.VirtualTimestamp
	err = db.Query().Exec(ctx,
		fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (2, \"execute\")", tablePath),
		query.WithTxControl(query.StrictSerializableReadWriteTxControl(query.CommitTx())),
		query.WithCommitTimestamp(func(timestamp *query.VirtualTimestamp) {
			executeTimestamp = timestamp
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, executeTimestamp)
	require.Positive(t, executeTimestamp.PlanStep())
	require.Positive(t, executeTimestamp.TxID())
	order, err := explicitTimestamp.Compare(*executeTimestamp)
	require.NoError(t, err)
	require.Less(t, order, 0)

	var streamTimestampBefore, streamTimestampAfter *query.VirtualTimestamp
	err = db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		result, err := session.Query(ctx,
			fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (3, \"stream\")", tablePath),
			query.WithTxControl(query.StrictSerializableReadWriteTxControl(query.CommitTx())),
		)
		if err != nil {
			return err
		}
		provider := result.(query.CommitTimestampProvider)
		streamTimestampBefore = provider.CommitTimestamp()
		if err := result.Close(ctx); err != nil {
			return err
		}
		streamTimestampAfter = provider.CommitTimestamp()

		return nil
	})
	require.NoError(t, err)
	require.Nil(t, streamTimestampBefore)
	require.NotNil(t, streamTimestampAfter)
	order, err = executeTimestamp.Compare(*streamTimestampAfter)
	require.NoError(t, err)
	require.Less(t, order, 0)

	materialized, err := db.Query().Query(ctx,
		fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (4, \"materialized\")", tablePath),
		query.WithTxControl(query.StrictSerializableReadWriteTxControl(query.CommitTx())),
	)
	require.NoError(t, err)
	defer func() { _ = materialized.Close(ctx) }()
	materializedTimestamp := materialized.(query.CommitTimestampProvider).CommitTimestamp()
	require.NotNil(t, materializedTimestamp)
	order, err = streamTimestampAfter.Compare(*materializedTimestamp)
	require.NoError(t, err)
	require.Less(t, order, 0)

	var defaultModeTimestamp *query.VirtualTimestamp
	err = db.Query().Exec(ctx,
		fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (5, \"serializable\")", tablePath),
		query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
		query.WithCommitTimestamp(func(timestamp *query.VirtualTimestamp) {
			defaultModeTimestamp = timestamp
		}),
	)
	require.NoError(t, err)
	require.Nil(t, defaultModeTimestamp)

	for id, want := range map[int64]string{
		1: "explicit", 2: "execute", 3: "stream", 4: "materialized", 5: "serializable",
	} {
		row, err := db.Query().QueryRow(ctx,
			fmt.Sprintf("SELECT val FROM `%s` WHERE id = %d", tablePath, id),
		)
		require.NoError(t, err)
		var got string
		require.NoError(t, row.Scan(&got))
		require.Equal(t, want, got)
	}
}

func TestQueryStrictSerializableReadOnlyHasNoCommitTimestamp(t *testing.T) {
	if os.Getenv("YDB_STRICT_SERIALIZABLE_ENABLED") != "1" {
		t.Skip("requires YDB TableServiceConfig.EnableStrictSerializableIsolation")
	}

	scope := newScope(t)
	db := scope.Driver(ydb.WithBalancer(balancers.SingleConn()))
	ctx, cancel := context.WithTimeout(scope.Ctx, 2*time.Minute)
	defer cancel()

	var executeTimestamp *query.VirtualTimestamp
	err := db.Query().Exec(ctx, "SELECT 1",
		query.WithTxControl(query.StrictSerializableReadWriteTxControl(query.CommitTx())),
		query.WithCommitTimestamp(func(timestamp *query.VirtualTimestamp) {
			executeTimestamp = timestamp
		}),
	)
	require.NoError(t, err)
	require.Nil(t, executeTimestamp)

	var explicitTx query.Transaction
	err = db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		tx, err := session.Begin(ctx, query.TxSettings(query.WithStrictSerializableReadWrite()))
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		explicitTx = tx
		if err := tx.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}

		return tx.CommitTx(ctx)
	})
	require.NoError(t, err)
	require.Nil(t, explicitTx.(query.CommitTimestampProvider).CommitTimestamp())
}
