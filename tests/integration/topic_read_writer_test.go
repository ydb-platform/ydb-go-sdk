//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestSendAsyncMessages(t *testing.T) {
	ctx := context.Background()
	db := connect(t)
	topicPath := createTopic(ctx, t, db)

	content := "hello"

	writer, err := db.Topic().StartWriter(topicPath)
	require.NoError(t, err)
	require.NotEmpty(t, writer)
	require.NoError(t, writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(content)}))

	reader, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
	require.NoError(t, err)

	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	mess, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	readBytes, err := io.ReadAll(mess)
	require.NoError(t, err)
	require.Equal(t, content, string(readBytes))
}

func TestSendSyncMessages(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)

		grpcStopper := NewGrpcStopper(errors.New("stop grpc for test"))

		db := connect(t,
			ydb.With(config.WithGrpcOptions(
				grpc.WithChainUnaryInterceptor(grpcStopper.UnaryClientInterceptor),
				grpc.WithChainStreamInterceptor(grpcStopper.StreamClientInterceptor),
			)),
		)
		topicPath := createTopic(ctx, t, db)

		writer, err := db.Topic().StartWriter(
			topicPath,
			topicoptions.WithSyncWrite(true),
		)
		require.NoError(t, err)
		msg := topicwriter.Message{Data: strings.NewReader("1")}
		err = writer.Write(ctx, msg)
		require.NoError(t, err)

		grpcStopper.Stop() // stop any activity through connections

		// check about connection broken
		msg = topicwriter.Message{CreatedAt: time.Now(), Data: strings.NewReader("nosent")}
		err = writer.Write(ctx, msg)
		require.Error(t, err)

		db = connect(t)
		writer, err = db.Topic().StartWriter(topicPath,
			topicoptions.WithSyncWrite(true),
		)
		require.NoError(t, err)
		msg = topicwriter.Message{Data: strings.NewReader("2")}
		err = writer.Write(ctx, msg)
		require.NoError(t, err)

		reader, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
		require.NoError(t, err)
		mess, err := reader.ReadMessage(ctx)
		require.NoError(t, err)
		require.NoError(t, topicsugar.ReadMessageDataWithCallback(mess, func(data []byte) error {
			require.Equal(t, "1", string(data))
			return nil
		}))
		mess, err = reader.ReadMessage(ctx)
		require.NoError(t, err)
		require.NoError(t, topicsugar.ReadMessageDataWithCallback(mess, func(data []byte) error {
			require.Equal(t, "2", string(data))
			return nil
		}))
	})
}

func TestMessageMetadata(t *testing.T) {
	t.Run("NoMetadata", func(t *testing.T) {
		e := newScope(t)
		err := e.TopicWriter().Write(e.Ctx, topicwriter.Message{})
		e.Require.NoError(err)

		mess, err := e.TopicReader().ReadMessage(e.Ctx)
		e.Require.NoError(err)
		e.Require.Nil(mess.Metadata)
	})
	t.Run("Meta1", func(t *testing.T) {
		if version.Lt(os.Getenv("YDB_VERSION"), "24.0") {
			t.Skip()
		}
		e := newScope(t)
		meta := map[string][]byte{
			"key": []byte("val"),
		}
		err := e.TopicWriter().Write(e.Ctx, topicwriter.Message{
			Metadata: meta,
		})
		e.Require.NoError(err)

		mess, err := e.TopicReader().ReadMessage(e.Ctx)
		e.Require.NoError(err)
		e.Require.Equal(meta, mess.Metadata)
	})
	t.Run("Meta2", func(t *testing.T) {
		if version.Lt(os.Getenv("YDB_VERSION"), "24.0") {
			t.Skip()
		}
		e := newScope(t)
		meta := map[string][]byte{
			"key1": []byte("val1"),
			"key2": []byte("val2"),
			"key3": []byte("val3"),
		}
		err := e.TopicWriter().Write(e.Ctx, topicwriter.Message{
			Metadata: meta,
		})
		e.Require.NoError(err)

		mess, err := e.TopicReader().ReadMessage(e.Ctx)
		e.Require.NoError(err)
		e.Require.Equal(meta, mess.Metadata)
	})
}

func TestManyConcurentReadersWriters(sourceTest *testing.T) {
	const partitionCount = 3
	const writersCount = 5
	const readersCount = 10
	const sendMessageCount = 100
	const totalMessageCount = sendMessageCount * writersCount

	t := xtest.MakeSyncedTest(sourceTest)
	ctx := xtest.Context(t)
	db := connect(t, ydb.WithLogger(
		newLogger(t),
		trace.DetailsAll,
	))

	// create topic
	topicName := t.Name()
	_ = db.Topic().Drop(ctx, topicName)
	err := db.Topic().Create(
		ctx,
		topicName,
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw),
		topicoptions.CreateWithMinActivePartitions(partitionCount),
	)
	require.NoError(t, err)

	// senders
	writer := func(producerID string) {
		pprof.Do(ctx, pprof.Labels("writer", producerID), func(ctx context.Context) {
			w, errWriter := db.Topic().StartWriter(topicName, topicoptions.WithProducerID(producerID))
			require.NoError(t, errWriter)

			for i := 0; i < sendMessageCount; i++ {
				buf := &bytes.Buffer{}
				errWriter = binary.Write(buf, binary.BigEndian, int64(i))
				require.NoError(t, errWriter)
				mess := topicwriter.Message{Data: buf}
				errWriter = w.Write(ctx, mess)
				require.NoError(t, errWriter)
			}
		})
	}
	for i := 0; i < writersCount; i++ {
		go writer(strconv.Itoa(i))
	}

	// readers
	type receivedMessT struct {
		ctx     context.Context
		writer  string
		content int64
	}
	readerCtx, readerCancel := context.WithCancel(ctx)
	receivedMessage := make(chan receivedMessT, totalMessageCount)

	reader := func(consumerID string) {
		pprof.Do(ctx, pprof.Labels("reader", consumerID), func(ctx context.Context) {
			r, errReader := db.Topic().StartReader(
				consumerID,
				topicoptions.ReadTopic(topicName),
				topicoptions.WithCommitTimeLagTrigger(0),
			)
			require.NoError(t, errReader)

			for {
				mess, errReader := r.ReadMessage(readerCtx)
				if readerCtx.Err() != nil {
					return
				}
				require.NoError(t, errReader)

				var val int64
				errReader = binary.Read(mess, binary.BigEndian, &val)
				require.NoError(t, errReader)
				receivedMessage <- receivedMessT{
					ctx:     mess.Context(),
					writer:  mess.ProducerID,
					content: val,
				}
				errReader = r.Commit(ctx, mess)
				require.NoError(t, errReader)
			}
		})
	}

	err = db.Topic().Alter(ctx, topicName, topicoptions.AlterWithAddConsumers(
		topictypes.Consumer{
			Name: commonConsumerName,
		},
	))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(readersCount)
	for i := 0; i < readersCount; i++ {
		go func(id int) {
			defer wg.Done()

			reader(commonConsumerName)
		}(i)
	}

	received := map[string]int64{}
	for i := 0; i < writersCount; i++ {
		received[strconv.Itoa(i)] = -1
	}

	cnt := 0
	doubles := 0
	for cnt < totalMessageCount {
		mess := <-receivedMessage
		stored := received[mess.writer]
		if mess.content <= stored {
			// double
			doubles++
			continue
		}
		cnt++
		require.Equal(t, stored+1, mess.content)
		received[mess.writer] = mess.content
	}

	// check about no more messages
	select {
	case mess := <-receivedMessage:
		t.Fatal(mess)
	default:
	}

	readerCancel()
	wg.Wait()
	t.Log(doubles)
}

func TestCommitUnexpectedRange(t *testing.T) {
	sleepTime := time.Second
	ctx := xtest.Context(t)
	db := connect(t)

	topicName1 := createTopic(ctx, t, db)
	topicName2 := createTopic(ctx, t, db)
	consumer := "test"
	err := addConsumer(ctx, db, topicName1, consumer)
	require.NoError(t, err)
	err = addConsumer(ctx, db, topicName2, consumer)
	require.NoError(t, err)

	// get range from other reader
	writer, err := db.Topic().StartWriter(topicName1)
	require.NoError(t, err)

	err = writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("123")})
	require.NoError(t, err)

	reader1, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topicName1))
	require.NoError(t, err)
	mess1, err := reader1.ReadMessage(ctx)
	require.NoError(t, err)

	connected := make(empty.Chan)

	tracer := trace.Topic{
		OnReaderInit: func(startInfo trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
			return func(doneInfo trace.TopicReaderInitDoneInfo) {
				close(connected)
			}
		},
	}

	reader, err := db.Topic().StartReader(
		consumer,
		topicoptions.ReadTopic(topicName2),
		topicoptions.WithReaderTrace(tracer),
	)
	require.NoError(t, err)

	<-connected

	err = reader.Commit(ctx, mess1)
	require.Error(t, err)

	readCtx, cancel := context.WithTimeout(ctx, sleepTime)
	defer cancel()
	_, err = reader.ReadMessage(readCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestUpdateToken(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.Driver()
	dbLogging := scope.DriverWithGRPCLogging()
	topicPath := scope.TopicPath()

	tokenInterval := time.Second
	reader, err := dbLogging.Topic().StartReader(
		scope.TopicConsumerName(),
		topicoptions.ReadTopic(topicPath),
		topicoptions.WithReaderUpdateTokenInterval(tokenInterval),
	)
	scope.Require.NoError(err)

	writer, err := db.Topic().StartWriter(
		topicPath,
		topicoptions.WithWriterProducerID("producer-id"),
		topicoptions.WithWriterUpdateTokenInterval(tokenInterval),
	)
	scope.Require.NoError(err)

	var wg sync.WaitGroup

	wg.Add(1)
	stopTopicActivity := atomic.Bool{}
	go func() {
		defer wg.Done()

		for i := 0; true; i++ {
			if stopTopicActivity.Load() {
				return
			}

			msgContent := []byte(strconv.Itoa(i))
			err = writer.Write(ctx, topicwriter.Message{Data: bytes.NewReader(msgContent)})
			scope.Require.NoError(err)
		}
	}()

	hasMessages := atomic.Bool{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; true; i++ {
			if stopTopicActivity.Load() {
				return
			}

			msg, err := reader.ReadMessage(ctx)
			scope.Require.NoError(err)
			scope.Require.NoError(reader.Commit(ctx, msg))
			hasMessages.Store(true)
		}
	}()

	start := time.Now()
	for i := 0; time.Since(start) < time.Second*10; i++ {
		t.Log(i)
		hasMessages.Store(false)
		xtest.SpinWaitConditionWithTimeout(t, nil, time.Second*10, hasMessages.Load)
		time.Sleep(tokenInterval)
	}

	stopTopicActivity.Store(true)

	activityStopped := make(empty.Chan)
	go func() {
		wg.Wait()
		close(activityStopped)
	}()
	xtest.WaitChannelClosed(t, activityStopped)
}

func TestTopicWriterWithManualPartitionSelect(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)
	topicPath := createTopic(ctx, t, db)

	writer, err := db.Topic().StartWriter(
		topicPath,
		topicoptions.WithPartitionID(0),
		topicoptions.WithSyncWrite(true),
	)
	require.NoError(t, err)
	err = writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("asd")})
	require.NoError(t, err)
}

func TestWriterFlushMessagesBeforeClose(t *testing.T) {
	s := newScope(t)
	ctx := s.Ctx
	writer, err := s.Driver().Topic().StartWriter(s.TopicPath(), topicoptions.WithWriterWaitServerAck(false))
	require.NoError(t, err)

	count := 1000
	for i := 0; i < count; i++ {
		require.NoError(t, writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(strconv.Itoa(i))}))
	}
	require.NoError(t, writer.Close(ctx))

	for i := 0; i < count; i++ {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		mess, err := s.TopicReader().ReadMessage(readCtx)
		cancel()
		require.NoError(t, err)

		messBody, err := io.ReadAll(mess)
		require.NoError(t, err)
		messBodyString := string(messBody)
		require.Equal(t, strconv.Itoa(i), messBodyString)
		cancel()
	}
}

var topicCounter int

func createTopic(ctx context.Context, t testing.TB, db *ydb.Driver) (topicPath string) {
	topicCounter++
	topicPath = db.Name() + "/" + t.Name() + "--test-topic-" + strconv.Itoa(topicCounter)
	_ = db.Topic().Drop(ctx, topicPath)
	err := db.Topic().Create(
		ctx,
		topicPath,
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw),
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName}),
	)
	require.NoError(t, err)

	return topicPath
}

func addConsumer(ctx context.Context, db *ydb.Driver, topicName, consumerName string) error {
	consumer := topictypes.Consumer{Name: consumerName}
	return db.Topic().Alter(ctx, topicName, topicoptions.AlterWithAddConsumers(consumer))
}
