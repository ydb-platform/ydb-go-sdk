//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestReadersWritersStress(t *testing.T) {
	ctx := xtest.Context(t)
	db := connect(t)

	topicPrefix := db.Name() + "/stress-topic-"
	consumerName := commonConsumerName

	writeTime := time.Second * 10
	topicCount := runtime.GOMAXPROCS(0)
	if topicCount > 10 {
		topicCount = 10
	}
	t.Log("topic count: ", topicCount)

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

	errGrp, grpCtx := errgroup.WithContext(ctx)
	for _, topicOuter := range topics {
		topicInner := topicOuter
		errGrp.Go(func() error {
			return stressTestInATopic(
				grpCtx,
				t,
				db,
				writeTime,
				topicInner,
				consumerName,
				writersPerTopic,
				readersPerTopic,
			)
		})
	}
	require.NoError(t, errGrp.Wait())
}

func stressTestInATopic(
	ctx context.Context,
	t testing.TB,
	db *ydb.Driver,
	testTime time.Duration,
	topicPath string,
	consumerName string,
	topicWriters, topicReaders int,
) error {
	maxMessagesInBatch := 5
	var mStatus sync.Mutex
	writeStatusWriterSeqno := map[string]int64{}
	readStatusWriterMaxSeqNo := map[string]int64{}

	var stopWrite atomic.Bool

	writeToTopic := func(ctx context.Context, producerID string, wg *sync.WaitGroup) (resErr error) {
		var writer *topicwriter.Writer

		defer func() {
			closeErr := writer.Close(context.Background())

			if resErr == nil && closeErr != nil {
				resErr = closeErr
			}

			wg.Done()
		}()

		writer, err := db.Topic().StartWriter(topicPath,
			topicoptions.WithProducerID(producerID),
			topicoptions.WithSyncWrite(true),
			topicoptions.WithWriterSetAutoSeqNo(false),
		)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		seqNo := int64(0)
		for !stopWrite.Load() {
			messageCount := rand.Intn(maxMessagesInBatch) + 1 //nolint:gosec
			var messages []topicwriter.Message
			for i := 0; i < messageCount; i++ {
				seqNo++
				message := topicwriter.Message{
					SeqNo: seqNo,
					Data:  strings.NewReader(strconv.FormatInt(seqNo, 10) + "-content"),
				}
				messages = append(messages, message)
			}
			err = writer.Write(ctx, messages...)
			if err != nil {
				return err
			}
			mStatus.Lock()
			writeStatusWriterSeqno[producerID] = seqNo
			mStatus.Unlock()
		}
		return nil
	}

	readFromTopic := func(ctx context.Context, wg *sync.WaitGroup) (resErr error) {
		var reader *topicreader.Reader
		defer func() {
			closeErr := reader.Close(context.Background())

			if resErr == nil && closeErr != nil {
				resErr = closeErr
			}

			if ctx.Err() != nil && errors.Is(resErr, context.Canceled) {
				resErr = nil
			}

			wg.Done()
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

			// store max readed seqno for every producer id
			mStatus.Lock()
			oldSeq := readStatusWriterMaxSeqNo[mess.ProducerID]
			if mess.SeqNo > oldSeq {
				readStatusWriterMaxSeqNo[mess.ProducerID] = mess.SeqNo
			}
			mStatus.Unlock()

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
	defer stopReader()

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

	xtest.SpinWaitProgressWithTimeout(t, time.Minute, func() (progressValue interface{}, finished bool) {
		time.Sleep(time.Millisecond)
		needReadMessages := int64(0)
		mStatus.Lock()
		for producerID, writtenSeqNo := range writeStatusWriterSeqno {
			readedSeqNo := readStatusWriterMaxSeqNo[producerID]
			needReadMessages += writtenSeqNo - readedSeqNo
		}
		mStatus.Unlock()
		return needReadMessages, needReadMessages == 0
	})

	stopReader()
	xtest.WaitGroup(t, &readersWG)

	for i := 0; i < topicReaders; i++ {
		err := <-readersError
		if err != nil {
			return err
		}
	}

	//nolint:gocritic
	//// check about all messages are committed
	// https://github.com/ydb-platform/ydb-go-sdk/issues/531
	//reader, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
	//if err != nil {
	//	return err
	//}
	//readWithTimeout, readCancel := context.WithTimeout(ctx, time.Second/10)
	//defer readCancel()
	//_, err = reader.ReadMessage(readWithTimeout)
	//t.Log("err: ", err)
	//require.ErrorIs(t, err, context.DeadlineExceeded)

	return nil
}
