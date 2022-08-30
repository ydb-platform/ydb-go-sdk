package topicwriterinternal

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

func TestMessageQueue_AddMessages(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessages(1, 3, 5, 3)))

		require.Equal(t, 4, q.lastWrittenIndex)

		require.Len(t, q.messagesByOrder, 4)
		require.Equal(t, newTestMessage(1), q.messagesByOrder[1])
		require.Equal(t, newTestMessage(3), q.messagesByOrder[2])
		require.Equal(t, newTestMessage(5), q.messagesByOrder[3])
		require.Equal(t, newTestMessage(3), q.messagesByOrder[4])

		require.Len(t, q.seqNoToOrderId, 3)
		require.Equal(t, newOrderIDsFIFO(1), q.seqNoToOrderId[1])
		require.Equal(t, newOrderIDsFIFO(2, 4), q.seqNoToOrderId[3])
		require.Equal(t, newOrderIDsFIFO(3), q.seqNoToOrderId[5])
	})
	t.Run("Closed", func(t *testing.T) {
		q := newMessageQueue()
		_ = q.Close(errors.New("err"))
		require.Error(t, q.AddMessages(newTestMessages(1, 3, 5)))
	})
	t.Run("OverflowIndex", func(t *testing.T) {
		q := newMessageQueue()
		q.lastWrittenIndex = maxInt - 1
		require.NoError(t, q.AddMessages(newTestMessages(1, 3, 5)))
		require.Len(t, q.messagesByOrder, 3)
		q.messagesByOrder[maxInt] = newTestMessage(1)
		q.messagesByOrder[minInt] = newTestMessage(3)
		q.messagesByOrder[minInt+1] = newTestMessage(5)
		require.Equal(t, minInt+1, q.lastWrittenIndex)
	})
}

func TestMessageQueue_Close(t *testing.T) {
	q := newMessageQueue()
	testErr := errors.New("test")
	require.NoError(t, q.Close(testErr))
	require.Error(t, q.Close(errors.New("second")))
	require.Equal(t, testErr, q.closedErr)
	require.True(t, q.closed)
	<-q.closedChan
}

func TestMessageQueue_GetMessages(t *testing.T) {
	ctx := context.Background()
	t.Run("Simple", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessages(1, 2)))
		require.NoError(t, q.AddMessages(newTestMessages(3, 4)))

		messages, err := q.GetMessagesForSend(ctx)
		require.NoError(t, err)
		require.Equal(t, newTestMessages(1, 2, 3, 4), messages)
	})

	t.Run("SendMessagesAfterStartWait", func(t *testing.T) {
		q := newMessageQueue()

		var err error
		var messages *messageWithDataContentSlice
		gotMessages := make(empty.Chan)
		go func() {
			messages, err = q.GetMessagesForSend(ctx)
			close(gotMessages)
		}()

		waitGetMessageStarted(&q)
		require.NoError(t, q.AddMessages(newTestMessages(1, 2, 3)))

		<-gotMessages
		require.NoError(t, err)
		require.Equal(t, newTestMessages(1, 2, 3), messages)
	})

	t.Run("Stress", func(t *testing.T) {
		iterations := 100000
		q := newMessageQueue()

		var lastSentSeqNo int64
		sendFinished := make(empty.Chan)
		fatalChan := make(chan string)

		go func() {
			sendRand := rand.New(rand.NewSource(0))
			for i := 0; i < iterations; i++ {
				count := sendRand.Intn(10) + 1
				m := newContentMessagesSlice()
				for k := 0; k < count; k++ {
					number := int(atomic.AddInt64(&lastSentSeqNo, 1))
					m.m = append(m.m, newTestMessage(number))
				}
				require.NoError(t, q.AddMessages(m))
			}
			close(sendFinished)
		}()

		readFinished := make(empty.Chan)
		var lastReadSeqNo int64

		readCtx, readCancel := context.WithCancel(ctx)
		defer readCancel()

		go func() {
			defer close(readFinished)

			for {
				messages, err := q.GetMessagesForSend(readCtx)
				if err != nil {
					break
				}

				for _, mess := range messages.m {
					if atomic.LoadInt64(&lastReadSeqNo)+1 != mess.SeqNo {
						fatalChan <- string(debug.Stack())
						return
					}
					atomic.StoreInt64(&lastReadSeqNo, mess.SeqNo)
				}
			}
		}()

		select {
		case <-sendFinished:
		case stack := <-fatalChan:
			t.Fatal(stack)
		}

		waitTimeout := time.Second * 10
		startWait := time.Now()
	waitReader:
		for {
			if atomic.LoadInt64(&lastReadSeqNo) == lastSentSeqNo {
				readCancel()
			}
			select {
			case <-readFinished:
				break waitReader
			case stack := <-fatalChan:
				t.Fatal(stack)
			default:
			}

			runtime.Gosched()
			if time.Since(startWait) > waitTimeout {
				t.Fatal()
			}
		}
	})

	t.Run("ClosedContext", func(t *testing.T) {
		closedCtx, cancel := context.WithCancel(ctx)
		cancel()

		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessages(1, 2)))

		_, err := q.GetMessagesForSend(closedCtx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("CallOnClosedQueue", func(t *testing.T) {
		q := newMessageQueue()
		_ = q.Close(errors.New("test"))
		_, err := q.GetMessagesForSend(ctx)
		require.Error(t, err)
	})

	t.Run("CloseContextAfterCall", func(t *testing.T) {
		q := newMessageQueue()
		q.notifyNewMessages()

		var err error
		gotErr := make(empty.Chan)
		go func() {
			_, err = q.GetMessagesForSend(ctx)
			close(gotErr)
		}()

		waitGetMessageStarted(&q)

		testErr := errors.New("test")
		require.NoError(t, q.Close(testErr))

		<-gotErr
		require.ErrorIs(t, err, testErr)
	})
}

func TestMessageQueue_ResetSentProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("Simple", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessages(1, 2, 3)))
		res1, err := q.GetMessagesForSend(ctx)
		require.NoError(t, err)

		q.ResetSentProgress()
		require.Equal(t, 0, q.lastSentIndex)
		require.Equal(t, 3, q.lastWrittenIndex)
		res2, err := q.GetMessagesForSend(ctx)
		require.NoError(t, err)
		require.Equal(t, res1, res2)
	})

	t.Run("Overflow", func(t *testing.T) {
		q := newMessageQueue()
		q.lastWrittenIndex = maxInt - 1
		q.lastSentIndex = q.lastWrittenIndex

		require.NoError(t, q.AddMessages(newTestMessages(1, 2, 3)))
		res1, err := q.GetMessagesForSend(ctx)
		require.NoError(t, err)

		q.ResetSentProgress()
		require.Equal(t, maxInt-1, q.lastSentIndex)
		require.Equal(t, minInt+1, q.lastWrittenIndex)
		res2, err := q.GetMessagesForSend(ctx)
		require.NoError(t, err)
		require.Equal(t, res1, res2)
	})
}

func TestIsFirstCycledIndexLess(t *testing.T) {
	table := []struct {
		name   string
		first  int
		second int
		result bool
	}{
		{
			name:   "smallPositivesFirstLess",
			first:  1,
			second: 2,
			result: true,
		},
		{
			name:   "smallPositivesEquals",
			first:  1,
			second: 1,
			result: false,
		},
		{
			name:   "smallPositivesFirstGreater",
			first:  2,
			second: 1,
			result: false,
		},
		{
			name:   "edgePositivesFirstLess",
			first:  minPositiveIndexWhichOrderLessThenNegative - 1,
			second: minPositiveIndexWhichOrderLessThenNegative,
			result: true,
		},
		{
			name:   "edgePositivesFirstGreater",
			first:  minPositiveIndexWhichOrderLessThenNegative,
			second: minPositiveIndexWhichOrderLessThenNegative - 1,
			result: false,
		},
		{
			name:   "overflowEdgeFirstPositive",
			first:  maxInt,
			second: minInt,
			result: true,
		},
		{
			name:   "overflowEdgeFirstNegative",
			first:  minInt,
			second: maxInt,
			result: false,
		},
		{
			name:   "nearZeroFirstNegativeSecondZero",
			first:  -1,
			second: 0,
			result: true,
		},
		{
			name:   "nearZeroFirstZeroSecondNegative",
			first:  0,
			second: -1,
			result: false,
		},
		{
			name:   "nearZeroFirstZeroSecondPositive",
			first:  0,
			second: 1,
			result: true,
		},
		{
			name:   "nearZeroFirstNegativeSecondPositive",
			first:  -1,
			second: 1,
			result: true,
		},
		{
			name:   "nearZeroFirstPositiveSecondNegative",
			first:  1,
			second: -1,
			result: false,
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.result, isFirstCycledIndexLess(test.first, test.second))
		})
	}
}

func TestMinMaxIntConst(t *testing.T) {
	v := maxInt
	v++
	require.Equal(t, minInt, v)
}

func TestSortIndexes(t *testing.T) {
	table := []struct {
		name     string
		source   []int
		expected []int
	}{
		{
			name:     "empty",
			source:   []int{},
			expected: []int{},
		},
		{
			name:     "usual",
			source:   []int{30, 1, 2},
			expected: []int{1, 2, 30},
		},
		{
			name:     "nearZero",
			source:   []int{0, 1, -1},
			expected: []int{-1, 0, 1},
		},
		{
			name:     "indexoverflow",
			source:   []int{minInt, minInt + 1, maxInt - 1, maxInt},
			expected: []int{maxInt - 1, maxInt, minInt, minInt + 1},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			sortMessageQueueIndexes(test.source)
			require.Equal(t, test.expected, test.source)
		})
	}
}

func TestQueuePanicOnOverflow(t *testing.T) {
	require.Panics(t, func() {
		q := newMessageQueue()
		q.messagesByOrder[123] = messageWithDataContent{}
		q.lastWrittenIndex = maxInt
		q.addMessage(messageWithDataContent{})
	})
}

func TestQueue_Ack(t *testing.T) {
	t.Run("First", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessages(1, 2, 5)))

		q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 2,
			},
		})
		expectedMap := map[int]messageWithDataContent{
			1: newTestMessage(1),
			3: newTestMessage(5),
		}
		require.Equal(t, expectedMap, q.messagesByOrder)
	})
	t.Run("OneOfDouble", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessages(1, 2, 2)))

		// remove first with the seqno
		q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 2,
			},
		})
		expectedMap := map[int]messageWithDataContent{
			1: newTestMessage(1),
			3: newTestMessage(2),
		}
		require.Equal(t, expectedMap, q.messagesByOrder)

		// remove second message
		q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 2,
			},
		})
		expectedMap = map[int]messageWithDataContent{
			1: newTestMessage(1),
		}
		require.Equal(t, expectedMap, q.messagesByOrder)
	})
	t.Run("Unexisted", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessages(1)))

		// remove first with the seqno
		q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 5,
			},
		})
		expectedMap := map[int]messageWithDataContent{
			1: newTestMessage(1),
		}
		require.Equal(t, expectedMap, q.messagesByOrder)
	})
}

func waitGetMessageStarted(q *messageQueue) {
	q.notifyNewMessages()
	for len(q.hasNewMessages) != 0 {
		runtime.Gosched()
	}
}
