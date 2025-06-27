//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestSessionCoreContextCancel(t *testing.T) {
	scope := newScope(t)
	tableName := fmt.Sprintf("/local/%s", t.Name())

	conn, err := grpc.NewClient(scope.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		require.NoError(t, err)
	}(conn)

	db := scope.Driver(ydb.WithSessionPoolSizeLimit(1), ydb.WithExecuteDataQueryOverQueryClient(true))
	queryClient := Ydb_Query_V1.NewQueryServiceClient(conn)

	_ = db.Table().Do(scope.Ctx, func(ctx context.Context, session table.Session) error {
		return session.DropTable(ctx, tableName)
	})

	err = db.Table().Do(scope.Ctx, func(ctx context.Context, session table.Session) error {
		return session.CreateTable(ctx, tableName,
			options.WithColumn("id", types.Optional(types.TypeUint64)),
			options.WithColumn("v", types.Optional(types.TypeUint64)),
			options.WithPrimaryKeyColumn("id"),
		)
	})
	require.NoError(t, err)

	writeTx := table.SerializableReadWriteTxControl(
		table.CommitTx(),
	)

	var sessionID string
	_ = db.Table().Do(scope.Ctx, func(ctx context.Context, session table.Session) error {
		query := fmt.Sprintf("UPSERT INTO %s (id) VALUES (123)", t.Name())
		_, res, err := session.Execute(ctx, writeTx, query, nil)
		require.NoError(t, err)
		err = res.Close()
		require.NoError(t, err)
		sessionID = session.ID()
		return nil
	})
	releaseSession := make(chan struct{})
	go func() {
		_ = db.Table().Do(scope.Ctx, func(ctx context.Context, session table.Session) error {
			require.Equal(t, session.ID(), sessionID)
			query := fmt.Sprintf("UPSERT INTO %s (id, v) VALUES (123, 123)", t.Name())
			_, _, _ = session.Execute(ctx, writeTx, query, nil)
			<-releaseSession
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	_, err = queryClient.DeleteSession(scope.Ctx, &Ydb_Query.DeleteSessionRequest{SessionId: sessionID})
	require.NoError(t, err)

	// after that, listenAttachStream should fail. now we return session to the pool

	close(releaseSession)

	time.Sleep(100 * time.Millisecond)

	// next query should not be context canceled because of closed session, but should get a new session

	err = db.Table().Do(scope.Ctx, func(ctx context.Context, session table.Session) error {
		query := fmt.Sprintf("UPSERT INTO %s (id, v) VALUES (124, 124)", t.Name())
		_, _, err = session.Execute(ctx, writeTx, query, nil)
		return err
	})
	require.NoError(t, err)
}
