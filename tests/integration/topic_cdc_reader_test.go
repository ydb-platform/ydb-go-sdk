//go:build !fast
// +build !fast

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
	scope := newScope(t)
	db := scope.Driver()
	tablePath := scope.TablePathBackticked()
	cdcPath := scope.TableCDCPath()
	syncReader := scope.TopicReaderSync(cdcPath)

	rowID := int64(0)
	sendCDCMessage := func() {
		query := fmt.Sprintf(`
DECLARE $id AS Int64;

INSERT INTO %s (id) VALUES ($id)`,
			tablePath,
		)
		err := db.Table().DoTx(scope.Ctx, func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, query, table.NewQueryParameters(
				table.ValueParam("$id", types.Int64Value(rowID)),
			))
			return err
		})
		scope.Require.NoError(err)
		rowID++
	}

	sendCDCMessage()

	msg, err := syncReader.ReadMessage(scope.Ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), msg.SeqNo)

	require.NoError(t, syncReader.Commit(scope.Ctx, msg))
	require.NoError(t, syncReader.Close(scope.Ctx))

	sendCDCMessage()
	sendCDCMessage()

	reader := scope.TopicReader(cdcPath)
	// read only no committed messages
	for i := 0; i < 2; i++ {
		msg, err = reader.ReadMessage(scope.Ctx)
		require.NoError(t, err)
		require.Equal(t, int64(i)+2, msg.SeqNo)
	}

	// and can't read more messages
	ctxTimeout, cancel := context.WithTimeout(scope.Ctx, time.Second/10)
	_, err = reader.ReadMessage(ctxTimeout)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCDCFeedSendTopicPathSameAsSubscribed(t *testing.T) {
	scope := newScope(t)

	db := scope.Driver()
	cdcPath := scope.TableCDCPath()

	reader := scope.TopicReader(cdcPath)

	// send cdc messages
	err := db.Table().DoTx(scope.Ctx, func(ctx context.Context, tx table.TransactionActor) error {
		query := fmt.Sprintf(`INSERT INTO %s (id) VALUES(1)`, scope.TablePathBackticked())
		_, err := tx.Execute(ctx, query, nil)
		return err
	})
	scope.Require.NoError(err)

	msg, err := reader.ReadMessage(scope.Ctx)
	scope.Require.NoError(err)
	scope.Require.Equal(cdcPath, msg.Topic())

	description, err := db.Topic().Describe(scope.Ctx, cdcPath)
	scope.Require.NoError(err)

	cdcName := path.Base(cdcPath)
	scope.Require.Equal(cdcName, description.Path)
}

func TestCDCInTableDescribe(t *testing.T) {

	t.Run("SchemeDescribePath", func(t *testing.T) {
		scope := newScope(t)
		db := scope.Driver()
		topicPath := scope.TableCDCPath()

		desc, err := db.Scheme().DescribePath(scope.Ctx, topicPath)
		require.NoError(t, err)
		require.True(t, desc.IsTopic())
	})

	t.Run("DescribeTable", func(t *testing.T) {
		scope := newScope(t)
		db := scope.Driver()
		topicPath := scope.TableCDCPath()

		err := db.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			tablePath := scope.TablePath()
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
