//go:build !fast
// +build !fast

package topic_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestSendAsyncMessages(t *testing.T) {
	xtest.AllowByFlag(t, "TEST_TOPIC_WRITER")

	ctx := context.Background()
	db := connect(t)
	topicPath := createTopic(ctx, t, db)

	content := "hello"

	producerID := "test-producer-ang-message-group"
	writer, err := db.Topic().StartWriter(producerID, topicPath,
		topicoptions.WithMessageGroupID(producerID),
	)
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
	xtest.AllowByFlag(t, "TEST_TOPIC_WRITER")

	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := testCtx(t)

		grpcStopper := topic.NewGrpcStopper(errors.New("stop grpc for test"))

		db := connect(t,
			grpc.WithChainUnaryInterceptor(grpcStopper.UnaryClientInterceptor),
			grpc.WithChainStreamInterceptor(grpcStopper.StreamClientInterceptor),
		)
		topicPath := createTopic(ctx, t, db)

		producerID := "test-producer"
		writer, err := db.Topic().StartWriter(
			producerID,
			topicPath,
			topicoptions.WithWriterPartitioning(
				topicwriter.NewPartitioningWithMessageGroupID(producerID),
			),
			topicoptions.WithSyncWrite(true),
		)
		require.NoError(t, err)
		msg := topicwriter.Message{CreatedAt: time.Now(), Data: strings.NewReader("1")}
		err = writer.Write(ctx, msg)
		require.NoError(t, err)

		grpcStopper.Stop() // stop any activity through connections

		// check about connection broken
		msg = topicwriter.Message{CreatedAt: time.Now(), Data: strings.NewReader("nosent")}
		err = writer.Write(ctx, msg)
		require.Error(t, err)

		db = connect(t)
		writer, err = db.Topic().StartWriter(producerID, topicPath,
			topicoptions.WithWriterPartitioning(
				topicwriter.NewPartitioningWithMessageGroupID(producerID),
			),
			topicoptions.WithSyncWrite(true),
		)
		require.NoError(t, err)
		msg = topicwriter.Message{CreatedAt: time.Now(), Data: strings.NewReader("2")}
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

func TestManyConcurentReadersWriters(t *testing.T) {
	xtest.AllowByFlag(t, "TEST_TOPIC_WRITER")

	const partitionCount = 3
	const writersCount = 5
	const readersCount = 10
	const sendMessageCount = 100
	const totalMessageCount = sendMessageCount * writersCount

	ctx := xtest.Context(t)
	db := connect(t)

	// create topic
	topicName := t.Name()
	_ = db.Topic().Drop(ctx, topicName)
	err := db.Topic().Create(
		ctx,
		topicName,
		[]topictypes.Codec{topictypes.CodecRaw},
		topicoptions.CreateWithMinActivePartitions(partitionCount),
	)
	require.NoError(t, err)

	// senders
	writer := func(producerID string) {
		pprof.Do(ctx, pprof.Labels("writer", producerID), func(ctx context.Context) {
			w, errWriter := db.Topic().StartWriter(producerID, topicName, topicoptions.WithMessageGroupID(producerID))
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

	consumerID := "consumer"
	err = db.Topic().Alter(ctx, topicName, topicoptions.AlterWithAddConsumers(
		topictypes.Consumer{
			Name: consumerID,
		},
	))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(readersCount)
	for i := 0; i < readersCount; i++ {
		go func(id int) {
			defer wg.Done()

			reader(consumerID)
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

func createTopic(ctx context.Context, t testing.TB, db ydb.Connection) (topicPath string) {
	topicPath = db.Name() + "/" + t.Name() + "--test-topic"
	_ = db.Topic().Drop(ctx, topicPath)
	err := db.Topic().Create(
		ctx,
		topicPath,
		[]topictypes.Codec{topictypes.CodecRaw},
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName}),
	)
	require.NoError(t, err)

	return topicPath
}
