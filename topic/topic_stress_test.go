package topic_test

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestReadersWritersStress(t *testing.T) {
	ctx := xtest.Context(t)
	xtest.AllowByFlag(t, "SHORT_STRESS_TEST")
	db := connect(t)

	topicPrefix := db.Name() + "/stress-topic-"
	consumerName := "consumer"

	writeTime := time.Second * 10
	topicCount := 10
	topicPartitions := 3
	writersPerTopic := topicPartitions * 2
	readersPerTopic := 2

	var topics []string
	for i := 0; i < topicCount; i++ {
		topicPath := topicPrefix + strconv.Itoa(i)
		_ = db.Topic().Drop(ctx, topicPath)
		err := db.Topic().Create(ctx, topicPath,
			topicoptions.CreateWithMinActivePartitions(int64(topicPartitions)),
			topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName}),
		)
		require.NoError(t, err)
		topics = append(topics, topicPath)
	}

	for _, topicOuter := range topics {
		topicInner := topicOuter
		t.Run(topicInner, func(t *testing.T) {
			t.Parallel()

			require.NoError(t, stressTestInATopic(ctx, t, db, writeTime, topicInner, consumerName, writersPerTopic, readersPerTopic))
		})
	}
}

func stressTestInATopic(ctx context.Context, t testing.TB, db ydb.Connection, testTime time.Duration, topicPath string, consumerName string, topicWriters, topicReaders int) error {
	maxMessagesInBatch := 5
	createdMessagesCount := int64(0)
	readedMessagesCount := int64(0)

	var stopWrite xatomic.Bool

	writeToTopic := func(ctx context.Context, producerID string, wg *sync.WaitGroup) error {
		defer wg.Done()

		writer, err := db.Topic().StartWriter(producerID, topicPath,
			topicoptions.WithMessageGroupID(producerID),
			topicoptions.WithSyncWrite(true),
		)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		for !stopWrite.Load() {
			messageCount := rand.Intn(maxMessagesInBatch) + 1
			var messages []topicwriter.Message
			for i := 0; i < messageCount; i++ {
				newMessageContent := atomic.AddInt64(&createdMessagesCount, 1)
				message := topicwriter.Message{
					Data: strings.NewReader(strconv.FormatInt(newMessageContent, 10)),
				}
				messages = append(messages, message)
			}
			err = writer.Write(ctx, messages...)
			if err != nil {
				return err
			}
		}
		return nil
	}

	readFromTopic := func(ctx context.Context, wg *sync.WaitGroup) (resErr error) {
		defer func() {
			wg.Done()

			if ctx.Err() != nil && errors.Is(resErr, context.Canceled) {
				resErr = nil
			}
		}()

		reader, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
		if err != nil {
			return err
		}

		for {
			mess, err := reader.ReadMessage(ctx)
			if err != nil {
				return err
			}

			atomic.AddInt64(&readedMessagesCount, 1)
			err = reader.Commit(ctx, mess)
			if err != nil {
				return err
			}
		}
	}

	var writersWG sync.WaitGroup
	writersErrors := make(chan error, topicWriters)
	for i := 0; i < topicWriters; i++ {
		producerID := "producer-" + strconv.Itoa(i)
		writersWG.Add(1)
		go func() {
			writersErrors <- writeToTopic(ctx, producerID, &writersWG)
		}()
	}

	var readersWG sync.WaitGroup
	readersError := make(chan error, topicReaders)
	readCtx, stopReader := context.WithCancel(ctx)
	for i := 0; i < topicReaders; i++ {
		readersWG.Add(1)
		go func() {
			readersError <- readFromTopic(readCtx, &readersWG)
		}()
	}

	time.Sleep(testTime)
	stopWrite.Store(true)

	xtest.WaitGroup(t, &writersWG)

	for i := 0; i < topicWriters; i++ {
		err := <-writersErrors
		if err != nil {
			return err
		}
	}

	xtest.SpinWaitConditionWithTimeout(t, nil, time.Minute, func() bool {
		return atomic.LoadInt64(&createdMessagesCount) == atomic.LoadInt64(&readedMessagesCount)
	})

	stopReader()
	xtest.WaitGroup(t, &readersWG)

	for i := 0; i < topicReaders; i++ {
		err := <-readersError
		if err != nil {
			return err
		}
	}

	return nil
}
