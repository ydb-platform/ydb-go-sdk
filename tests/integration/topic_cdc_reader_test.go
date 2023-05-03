//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const (
	consumerName = "test-consumer"
)

func TestReadMessagesAndCommit(t *testing.T) {
	ctx := xtest.Context(t)

	db, reader := createFeedAndReader(ctx, t, topicoptions.WithCommitMode(topicoptions.CommitModeSync))

	sendCDCMessage(ctx, t, db)

	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), msg.SeqNo)

	require.NoError(t, reader.Commit(ctx, msg))
	require.NoError(t, reader.Close(ctx))

	sendCDCMessage(ctx, t, db)
	sendCDCMessage(ctx, t, db)
	reader = createFeedReader(t, db)

	// read only no committed messages
	for i := 0; i < 2; i++ {
		msg, err = reader.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(i)+2, msg.SeqNo)
	}

	// and can't read more messages
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second/10)
	_, err = reader.ReadMessage(ctxTimeout)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCDCFeedSendTopicPathSameAsSubscribed(t *testing.T) {
	ctx := xtest.Context(t)

	db, reader := createFeedAndReader(ctx, t)

	topicName := "feed"
	topicPath := db.Name() + "/test/feed"

	t.Run("ReceivedMessage", func(t *testing.T) {
		sendCDCMessage(ctx, t, db)

		msg, err := reader.ReadMessage(ctx)
		require.NoError(t, err)

		require.Equal(t, topicPath, msg.Topic())
	})
	t.Run("Describe", func(t *testing.T) {
		res, err := db.Topic().Describe(ctx, topicPath)
		require.NoError(t, err)
		require.Equal(t, topicName, res.Path)
	})
}

func TestTopicPath(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)

	topicPath := db.Name() + "/" + t.Name()
	_ = db.Topic().Drop(ctx, topicPath)

	err := db.Topic().Create(ctx, topicPath)
	require.NoError(t, err)
}

func TestCDCInTableDescribe(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)
	topicPath := createCDCFeed(ctx, t, db)

	t.Run("SchemeDescribePath", func(t *testing.T) {
		desc, err := db.Scheme().DescribePath(ctx, topicPath)
		require.NoError(t, err)
		require.True(t, desc.IsTopic())
	})

	t.Run("DescribeTable", func(t *testing.T) {
		err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			tablePath := path.Dir(topicPath)
			topicName := path.Base(topicPath)
			desc, err := s.DescribeTable(ctx, tablePath)
			if err != nil {
				return err
			}
			if topicName != desc.Changefeeds[0].Name {
				return fmt.Errorf("unexpected topic name: %s, epx: %s", desc.Changefeeds[0].Name, topicName)
			}
			return nil
		}, table.WithIdempotent())
		require.NoError(t, err)
	})
}

func createCDCFeed(ctx context.Context, t *testing.T, db *ydb.Driver) string {
	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_ = s.ExecuteSchemeQuery(ctx, "DROP TABLE test")
		err := s.ExecuteSchemeQuery(ctx, `
			CREATE TABLE
				test
			(
				id Int64,
				val Utf8,
				PRIMARY KEY (id)
			)`,
		)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		err = s.ExecuteSchemeQuery(ctx, `
			ALTER TABLE
				test
			ADD CHANGEFEED
				feed
			WITH (
				FORMAT = 'JSON',
				MODE = 'UPDATES'
			)`,
		)
		if err != nil {
			return fmt.Errorf("failed to add changefeed: %w", err)
		}

		return nil
	}, table.WithIdempotent())
	require.NoError(t, err)

	topicPath := testCDCFeedName(db)

	require.NoError(t, err)

	err = db.Topic().Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: consumerName}),
	)
	require.NoError(t, err)
	return topicPath
}

func createFeedReader(t *testing.T, db *ydb.Driver, opts ...topicoptions.ReaderOption) *topicreader.Reader {
	topicPath := testCDCFeedName(db)
	reader, err := db.Topic().StartReader(consumerName, []topicoptions.ReadSelector{
		{
			Path: topicPath,
		},
	}, opts...)
	require.NoError(t, err)
	return reader
}

func createFeedAndReader(
	ctx context.Context,
	t *testing.T,
	opts ...topicoptions.ReaderOption,
) (*ydb.Driver, *topicreader.Reader) {
	db := connect(t)
	createCDCFeed(ctx, t, db)
	reader := createFeedReader(t, db, opts...)
	return db, reader
}

var sendCDCCounter int64

func sendCDCMessage(ctx context.Context, t *testing.T, db *ydb.Driver) {
	counter := atomic.AddInt64(&sendCDCCounter, 1)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(ctx,
			"DECLARE $id AS Int64; INSERT INTO test (id, val) VALUES($id, 'asd')",
			table.NewQueryParameters(table.ValueParam("$id", types.Int64Value(counter))))
		return err
	})
	require.NoError(t, err)
}

func testCDCFeedName(db *ydb.Driver) string {
	return db.Name() + "/test/feed"
}
