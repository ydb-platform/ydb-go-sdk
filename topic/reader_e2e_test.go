//go:build !fast
// +build !fast

package topic_test

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	consumerName = "test-consumer"
)

func TestReadMessages(t *testing.T) {
	ctx := testCtx(t)

	db, reader := createFeedAndReader(ctx, t)
	defer func() {
		_ = reader.Close(ctx)
		_ = db.Close(ctx)
	}()

	sendCDCMessage(ctx, t, db)
	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, msg.CreatedAt)
	t.Logf("msg: %#v", msg)

	require.NoError(t, err)
	err = topicsugar.ReadMessageDataWithCallback(msg, func(data []byte) error {
		t.Log("Content:", string(data))
		return nil
	})
	require.NoError(t, err)

	sendCDCMessage(ctx, t, db)
	batch, err := reader.ReadMessageBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)
}

func TestReadMessagesAndCommit(t *testing.T) {
	ctx := testCtx(t)

	db, reader := createFeedAndReader(ctx, t, topicoptions.WithCommitMode(topicoptions.CommitModeSync))
	defer func() {
		_ = reader.Close(ctx)
		_ = db.Close(ctx)
	}()

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
	ctx := testCtx(t)

	db, reader := createFeedAndReader(ctx, t)
	defer func() {
		_ = reader.Close(ctx)
		_ = db.Close(ctx)
	}()

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
	ctx := testCtx(t)
	db := connect(t)

	topicPath := db.Name() + "/" + t.Name()
	_ = db.Topic().Drop(ctx, topicPath)

	err := db.Topic().Create(ctx, topicPath, []topictypes.Codec{topictypes.CodecRaw})
	require.NoError(t, err)
}

func TestPartitionsBalanced(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := testCtx(t)
		db := connect(t)
		defer func() {
			_ = db.Close(context.Background())
		}()
		topicPath := db.Name() + "/topic-" + t.Name()

		err := db.Topic().Drop(ctx, topicPath)
		if err != nil {
			require.True(t, ydb.IsOperationErrorSchemeError(err))
		}

		consumer := "test"
		err = db.Topic().Create(ctx, topicPath, []topictypes.Codec{topictypes.CodecRaw},
			topicoptions.CreateWithMinActivePartitions(2),
			topicoptions.CreateWithPartitionCountLimit(2),
			topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumer}),
		)
		require.NoError(t, err)

		connectedPartitions := int32(0)
		var handled int32

		var sessionsMutex sync.Mutex
		sessions := map[int64]bool{}

		tracer := trace.Topic{
			OnReaderPartitionReadStartResponse: func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) { //nolint:lll
				atomic.StoreInt32(&handled, 1)

				atomic.AddInt32(&connectedPartitions, 1)
				return nil
			},
			OnReaderPartitionReadStopResponse: func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) { //nolint:lll
				atomic.StoreInt32(&handled, 1)

				sessionsMutex.Lock()
				defer sessionsMutex.Unlock()
				if sessions[startInfo.PartitionSessionID] {
					return nil
				}
				sessions[startInfo.PartitionSessionID] = true

				atomic.AddInt32(&connectedPartitions, -1)
				return nil
			},
		}
		firstReader, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath),
			topicoptions.WithReaderTrace(tracer),
		)
		require.NoError(t, err)

		readCtx, firstReaderStopRead := context.WithCancel(ctx)
		firstReaderReadStopped := make(empty.Chan)
		go func() {
			defer close(firstReaderReadStopped)

			for {
				if readCtx.Err() != nil {
					return
				}
				_, err = firstReader.ReadMessage(readCtx)
				if readCtx.Err() == nil {
					require.NoError(t, err)
				}
			}
		}()

		xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
			return atomic.LoadInt32(&connectedPartitions) == 2
		})

		readerSecond, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicPath))
		require.NoError(t, err)

		xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
			return atomic.LoadInt32(&connectedPartitions) == 1
		})

		require.NoError(t, readerSecond.Close(ctx))

		xtest.SpinWaitConditionWithTimeout(t, nil, time.Second, func() bool {
			return atomic.LoadInt32(&connectedPartitions) == 2
		})

		firstReaderStopRead()
		xtest.WaitChannelClosed(t, firstReaderReadStopped)
		require.NoError(t, firstReader.Close(ctx))
	})
}

func createCDCFeed(ctx context.Context, t *testing.T, db ydb.Connection) {
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
	})
	require.NoError(t, err)

	topicPath := testCDCFeedName(db)

	require.NoError(t, err)

	err = db.Topic().Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: consumerName}),
	)
	require.NoError(t, err)
}

func createFeedReader(t *testing.T, db ydb.Connection, opts ...topicoptions.ReaderOption) *topicreader.Reader {
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
) (ydb.Connection, *topicreader.Reader) {
	db := connect(t)
	createCDCFeed(ctx, t, db)
	reader := createFeedReader(t, db, opts...)
	return db, reader
}

var sendCDCCounter int64

func sendCDCMessage(ctx context.Context, t *testing.T, db ydb.Connection) {
	counter := atomic.AddInt64(&sendCDCCounter, 1)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		if _, err := tx.Execute(ctx,
			"DECLARE $id AS Int64; INSERT INTO test (id, val) VALUES($id, 'asd')",
			table.NewQueryParameters(table.ValueParam("$id", types.Int64Value(counter)))); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
}

func testCtx(t testing.TB) context.Context {
	ctx, cancel := xcontext.WithErrCancel(context.Background())
	t.Cleanup(func() {
		cancel(fmt.Errorf("ydb e2e test finished: %v", t.Name()))

		pprof.SetGoroutineLabels(ctx)
	})

	pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("test", t.Name())))
	return ctx
}

func testCDCFeedName(db ydb.Connection) string {
	return db.Name() + "/test/feed"
}
