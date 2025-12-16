//go:build integration
// +build integration

package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestSingleTransaction(t *testing.T) {
	scope := newScope(t)
	writer := scope.TopicWriter()
	reader := scope.TopicReader()
	driver := scope.Driver()
	ctx := scope.Ctx

	writeMessage(t, ctx, writer, "1")

	var batch *topicreader.Batch

	var once sync.Once
	err := driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		once.Do(func() {
			deleteTxSession(t, ctx, driver, tr.(tx.Transaction))
		})

		var err error
		batch, err = reader.PopMessagesBatchTx(ctx, tr)
		return err
	})

	require.NoError(t, err)
	require.Len(t, batch.Messages, 1)
	msgEqualString(t, "1", batch.Messages[0])
}

func TestSeveralReads(t *testing.T) {
	scope := newScope(t)
	writer := scope.TopicWriter()
	reader := scope.TopicReader()
	driver := scope.Driver()
	ctx := scope.Ctx

	writeMessage(t, ctx, writer, "1")

	err := driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		_, err := reader.PopMessagesBatchTx(ctx, tr)
		require.NoError(t, err)

		writeMessage(t, ctx, writer, "2")

		_, err = reader.PopMessagesBatchTx(ctx, tr)
		require.NoError(t, err)

		writeMessage(t, ctx, writer, "3")

		return nil
	})
	require.NoError(t, err)

	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	msgEqualString(t, "3", msg)
}

func TestSeveralTransactions(t *testing.T) {
	scope := newScope(t)
	writer := scope.TopicWriter()
	reader := scope.TopicReader()
	driver := scope.Driver()
	ctx := scope.Ctx

	writeMessage(t, ctx, writer, "1")

	var (
		once1 sync.Once
		once2 sync.Once
	)

	err := driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		once1.Do(func() {
			deleteTxSession(t, ctx, driver, tr.(tx.Transaction))
		})

		_, err := reader.PopMessagesBatchTx(ctx, tr)
		return err
	})
	require.NoError(t, err)

	writeMessage(t, ctx, writer, "2")

	var batch *topicreader.Batch
	err = driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		once2.Do(func() {
			deleteTxSession(t, ctx, driver, tr.(tx.Transaction))
		})

		var err error
		batch, err = reader.PopMessagesBatchTx(ctx, tr)
		return err
	})
	require.NoError(t, err)

	msgEqualString(t, "2", batch.Messages[0])
}

// Helper methods

func writeMessage(t *testing.T, ctx context.Context, writer *topicwriter.Writer, msg string) {
	t.Helper()
	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(msg)})
	require.NoError(t, err)
}

func msgEqualString(t *testing.T, expected string, msg *topicreader.Message) {
	t.Helper()

	var actual string

	topicsugar.ReadMessageDataWithCallback(msg, func(data []byte) error {
		actual = string(data)
		return nil
	})

	require.Equal(t, expected, actual)
}

func deleteTxSession(t *testing.T, ctx context.Context, driver *ydb.Driver, tx tx.Transaction) {
	t.Helper()
	deleteSession(t, ctx, driver, tx.SessionID())
}

func deleteSession(t *testing.T, ctx context.Context, driver *ydb.Driver, sessionID string) {
	t.Helper()
	_, err := Ydb_Query_V1.NewQueryServiceClient(ydb.GRPCConn(driver)).
		DeleteSession(ctx, &Ydb_Query.DeleteSessionRequest{
			SessionId: sessionID,
		})
	require.NoError(t, err)
}
