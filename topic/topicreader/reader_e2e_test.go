//go:build !fast
// +build !fast

package topicreader_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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
	err = msg.UnmarshalTo(topicsugar.ConsumeWithCallback(func(data []byte) error {
		t.Log("Content:", string(data))
		return nil
	}))
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
	reader = createReader(ctx, t, db)

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
	t.Skip("LOGBROKER-7625")
	t.Skip("KIKIMR-14966")

	ctx := testCtx(t)
	db := connect(ctx, t)

	topicPath := db.Name() + "/" + t.Name()
	_ = db.Topic().Drop(ctx, topicPath)

	err := db.Topic().Create(ctx, topicPath, []topictypes.Codec{topictypes.CodecRaw})
	require.NoError(t, err)
}

func TestPartitionsBalanced(t *testing.T) {
	ctx := testCtx(t)
	db := connect(ctx, t)
	topicPath := db.Name() + "/topic-" + t.Name()

	_ = db.Topic().Drop(ctx, topicPath)
	err := db.Topic().Create(ctx, topicPath, []topictypes.Codec{topictypes.CodecRaw})
	require.NoError(t, err)
}

func connect(ctx context.Context, t *testing.T) ydb.Connection {
	token := os.Getenv("YDB_TOKEN")

	connectionString := "grpc://localhost:2136?database=/local"
	if ecs := os.Getenv("YDB_CONNECTION_STRING"); ecs != "" {
		connectionString = ecs
	}

	db, err := ydb.Open(ctx, connectionString, ydb.WithAccessTokenCredentials(token))
	require.NoError(t, err)

	return db
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
)
	`)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		require.NoError(t, err)
		err = s.ExecuteSchemeQuery(ctx, `
ALTER TABLE
	test
ADD CHANGEFEED
	feed
WITH (
	FORMAT = 'JSON',
	MODE = 'UPDATES'
)
	`)
		if err != nil {
			return fmt.Errorf("failed to add changefeed: %w", err)
		}
		return nil
	})
	require.NoError(t, err)

	topicPath := db.Name() + "/test/feed"

	require.NoError(t, err)

	err = db.Topic().Alter(
		ctx,
		topicPath,
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{Name: "test"}),
	)
	require.NoError(t, err)
}

func createReader(
	ctx context.Context,
	t *testing.T,
	db ydb.Connection,
	opts ...topicoptions.ReaderOption,
) *topicreader.Reader {
	js := func(v interface{ JSONData() io.Reader }) string {
		data, err := io.ReadAll(v.JSONData())
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	tracer := trace.Topic{
		OnReadStreamRawSent: func(info trace.OnReadStreamRawSentInfo) {
			t.Logf("sent: %v %v\n%v", info.ClientMessage.Type(), info.Error, js(info.ClientMessage))
		},
		OnReadStreamRawReceived: func(info trace.OnReadStreamRawReceivedInfo) {
			t.Logf("received: %v %v\n%v", info.ServerMessage.Type(), info.Error, js(info.ServerMessage))
		},
	}
	_ = tracer

	opts = append(opts[:len(opts):len(opts)], topicoptions.WithTracer(tracer))

	topicPath := db.Name() + "/test/feed"
	reader, err := db.Topic().StartReader("test", []topicoptions.ReadSelector{
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
	db := connect(ctx, t)
	createCDCFeed(ctx, t, db)
	reader := createReader(ctx, t, db, opts...)
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

func testCtx(t *testing.T) context.Context {
	ctx, cancel := xcontext.WithErrCancel(context.Background())
	t.Cleanup(func() {
		cancel(fmt.Errorf("ydb e2e test finished: %v", t.Name()))

		pprof.SetGoroutineLabels(ctx)
	})

	pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("test", t.Name())))
	return ctx
}
