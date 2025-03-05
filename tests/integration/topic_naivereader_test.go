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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreadernaive"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicNaiveReadMessages(t *testing.T) {
	ctx := xtest.Context(t)

	db, reader := createFeedAndNaiveReader(ctx, t)

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

func TestTopicWriterLogMessagesWithoutDataNaiveReader(t *testing.T) {
	scope := newScope(t)

	producerID := "dkeujsl"
	const seqNoInt = 486812497
	seqNoString := "486812497"
	data := "kdjwkruowe"
	metaKey := "gyoeexiufo"
	metaValue := "fjedeikeosbv"

	logs := &strings.Builder{}
	writer, err := scope.Driver().Topic().StartWriter(
		scope.TopicPath(),
		topicoptions.WithWriterProducerID(producerID),
		topicoptions.WithWriterSetAutoSeqNo(false),
		topicoptions.WithWriterWaitServerAck(true),
		topicoptions.WithWriterTrace(log.Topic(
			log.Default(logs, log.WithMinLevel(log.TRACE)), trace.TopicWriterStreamGrpcMessageEvents),
		),
	)

	scope.Require.NoError(err)
	err = writer.Write(scope.Ctx,
		topicwriter.Message{
			SeqNo: seqNoInt,
			Data:  strings.NewReader(data),
			Metadata: map[string][]byte{
				metaKey: []byte(metaValue),
			},
		},
	)
	scope.Require.NoError(err)

	err = writer.Close(scope.Ctx)
	scope.Require.NoError(err)

	logsString := logs.String()
	scope.Require.Contains(logsString, producerID)
	scope.Require.Contains(logsString, seqNoString)
	scope.Require.NotContains(logsString, metaKey)
	scope.Require.NotContains(logsString, metaValue)
	scope.Require.NotContains(logsString, data)

	mess, err := scope.TopicNaiveReader().ReadMessage(scope.Ctx)
	scope.Require.NoError(err)

	scope.Require.Equal(producerID, mess.ProducerID)
	scope.Require.Equal(int64(seqNoInt), mess.SeqNo)
	scope.Require.Equal(metaValue, string(mess.Metadata[metaKey]))

	messData, err := io.ReadAll(mess)
	scope.Require.NoError(err)
	scope.Require.Equal(data, string(messData))
}

func TestSendAsyncMessagesNaiveReader(t *testing.T) {
	ctx := context.Background()
	db := connect(t)
	topicPath := createTopic(ctx, t, db)

	content := "hello"

	writer, err := db.Topic().StartWriter(topicPath)
	require.NoError(t, err)
	require.NotEmpty(t, writer)
	require.NoError(t, writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(content)}))

	reader := topicreadernaive.NewNaiveReader(db, consumerName, topicoptions.ReadTopic(topicPath))
	require.NoError(t, err)

	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	mess, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	readBytes, err := io.ReadAll(mess)
	require.NoError(t, err)
	require.Equal(t, content, string(readBytes))
}

func TestSendSyncMessagesNaiveReader(t *testing.T) {
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

		reader := topicreadernaive.NewNaiveReader(db, consumerName, topicoptions.ReadTopic(topicPath))
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

func TestMessageMetadataNaiveReader(t *testing.T) {
	t.Run("NoMetadata", func(t *testing.T) {
		e := newScope(t)
		err := e.TopicWriter().Write(e.Ctx, topicwriter.Message{})
		e.Require.NoError(err)

		mess, err := e.TopicNaiveReader().ReadMessage(e.Ctx)
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

		mess, err := e.TopicNaiveReader().ReadMessage(e.Ctx)
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

		mess, err := e.TopicNaiveReader().ReadMessage(e.Ctx)
		e.Require.NoError(err)
		e.Require.Equal(meta, mess.Metadata)
	})
}

func TestManyConcurentNaiveReadersWriters(sourceTest *testing.T) {
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
			r := topicreadernaive.NewNaiveReader(db, commonConsumerName, topicoptions.ReadTopic(topicName))

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

func TestWriterFlushMessagesBeforeCloseNaiveReader(t *testing.T) {
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
		mess, err := s.TopicNaiveReader().ReadMessage(readCtx)
		cancel()
		require.NoError(t, err)

		messBody, err := io.ReadAll(mess)
		require.NoError(t, err)
		messBodyString := string(messBody)
		require.Equal(t, strconv.Itoa(i), messBodyString)
		cancel()
	}
}

func createFeedAndNaiveReader(
	ctx context.Context,
	t *testing.T,
	opts ...topicoptions.ReaderOption,
) (*ydb.Driver, *topicreadernaive.NaiveReader) {
	db := connect(t)
	createCDCFeed(ctx, t, db)
	reader := createNaiveFeedReader(t, db, opts...)
	return db, reader
}

func createNaiveFeedReader(t *testing.T, db *ydb.Driver, opts ...topicoptions.ReaderOption) *topicreadernaive.NaiveReader {
	topicPath := testCDCFeedName(db)
	reader := topicreadernaive.NewNaiveReader(db, consumerName, topicoptions.ReadTopic(topicPath))
	return reader
}
