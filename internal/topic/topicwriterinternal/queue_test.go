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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func TestMessageQueue_AddMessages(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 3, 5)))

		require.Equal(t, 3, q.lastWrittenIndex)

		expected := map[int]messageWithDataContent{
			1: newTestMessageWithDataContent(1),
			2: newTestMessageWithDataContent(3),
			3: newTestMessageWithDataContent(5),
		}
		require.Equal(t, expected, q.messagesByOrder)

		require.Len(t, q.seqNoToOrderID, 3)
		require.Equal(t, 1, q.seqNoToOrderID[1])
		require.Equal(t, 2, q.seqNoToOrderID[3])
		require.Equal(t, 3, q.seqNoToOrderID[5])
	})
	t.Run("Closed", func(t *testing.T) {
		q := newMessageQueue()
		_ = q.Close(errors.New("err"))
		require.Error(t, q.AddMessages(newTestMessagesWithContent(1, 3, 5)))
	})
	t.Run("OverflowIndex", func(t *testing.T) {
		q := newMessageQueue()
		q.lastWrittenIndex = maxInt - 1
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 3, 5)))
		require.Len(t, q.messagesByOrder, 3)
		q.messagesByOrder[maxInt] = newTestMessageWithDataContent(1)
		q.messagesByOrder[minInt] = newTestMessageWithDataContent(3)
		q.messagesByOrder[minInt+1] = newTestMessageWithDataContent(5)
		require.Equal(t, minInt+1, q.lastWrittenIndex)
	})
	t.Run("BadOrder", func(t *testing.T) {
		q := newMessageQueue()
		require.Error(t, q.AddMessages(newTestMessagesWithContent(2, 1)))
	})
}

func TestMessageQueue_CheckMessages(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.checkNewMessagesBeforeAddNeedLock(newTestMessagesWithContent()))
	})
	t.Run("Unordered", func(t *testing.T) {
		q := newMessageQueue()
		require.Error(t, q.checkNewMessagesBeforeAddNeedLock(newTestMessagesWithContent(2, 2)))
		require.Error(t, q.checkNewMessagesBeforeAddNeedLock(newTestMessagesWithContent(2, 1)))
	})
	t.Run("NoGreaterThenLastSent", func(t *testing.T) {
		q := newMessageQueue()
		q.lastSeqNo = 10
		require.Error(t, q.checkNewMessagesBeforeAddNeedLock(newTestMessagesWithContent(int(q.lastSeqNo-1))))
		require.Error(t, q.checkNewMessagesBeforeAddNeedLock(newTestMessagesWithContent(int(q.lastSeqNo))))
		require.NoError(t, q.checkNewMessagesBeforeAddNeedLock(newTestMessagesWithContent(int(q.lastSeqNo+1))))
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
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 2)))
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(3, 4)))

		messages, err := q.GetMessagesForSend(ctx)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3, 4}, getSeqNumbers(messages))
	})

	t.Run("SendMessagesAfterStartWait", func(t *testing.T) {
		q := newMessageQueue()

		var err error
		var messages []messageWithDataContent
		gotMessages := make(empty.Chan)
		go func() {
			messages, err = q.GetMessagesForSend(ctx)
			close(gotMessages)
		}()

		waitGetMessageStarted(&q)
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 2, 3)))

		<-gotMessages
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3}, getSeqNumbers(messages))
	})

	t.Run("Stress", func(t *testing.T) {
		iterations := 100000
		q := newMessageQueue()

		var lastSentSeqNo int64
		sendFinished := make(empty.Chan)
		fatalChan := make(chan string)

		go func() {
			//nolint:gosec
			sendRand := rand.New(rand.NewSource(0))
			for range iterations {
				count := sendRand.Intn(10) + 1
				m := make([]messageWithDataContent, 0, count)
				for range count {
					number := int(atomic.AddInt64(&lastSentSeqNo, 1))
					m = append(m, newTestMessageWithDataContent(number))
				}
				require.NoError(t, q.AddMessages(m))
			}
			close(sendFinished)
		}()

		readFinished := make(empty.Chan)
		var lastReadSeqNo atomic.Int64

		readCtx, readCancel := xcontext.WithCancel(ctx)
		defer readCancel()

		go func() {
			defer close(readFinished)

			for {
				messages, err := q.GetMessagesForSend(readCtx)
				if err != nil {
					break
				}

				for _, mess := range messages {
					if lastReadSeqNo.Load()+1 != mess.SeqNo {
						fatalChan <- string(debug.Stack())

						return
					}
					lastReadSeqNo.Store(mess.SeqNo)
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
			if lastReadSeqNo.Load() == lastSentSeqNo {
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
		closedCtx, cancel := xcontext.WithCancel(ctx)
		cancel()

		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 2)))

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
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 2, 3)))
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

		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 2, 3)))
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
		q.addMessageNeedLock(messageWithDataContent{})
	})
}

func TestRegressionIssue1038_ReceiveAckAfterCloseQueue(t *testing.T) {
	counter := 0

	q := newMessageQueue()
	q.OnAckReceived = func(count int) {
		counter -= count
	}
	require.NoError(t, q.AddMessages(newTestMessagesWithContent(1)))
	counter++

	require.NoError(t, q.Close(errors.New("test err")))
	require.ErrorIs(t, q.AcksReceived([]rawtopicwriter.WriteAck{
		{
			SeqNo:              1,
			MessageWriteStatus: rawtopicwriter.MessageWriteStatus{},
		},
	}), errAckOnClosedMessageQueue)
	require.Zero(t, counter)
}

func TestQueue_Ack(t *testing.T) {
	t.Run("First", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 2, 5)))

		require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 2,
			},
		}))
		expectedMap := map[int]messageWithDataContent{
			1: newTestMessageWithDataContent(1),
			3: newTestMessageWithDataContent(5),
		}
		require.Equal(t, expectedMap, q.messagesByOrder)
	})
	t.Run("Unexisted", func(t *testing.T) {
		q := newMessageQueue()
		require.NoError(t, q.AddMessages(newTestMessagesWithContent(1)))

		// remove first with the seqno
		require.Error(t, q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 5,
			},
		}))

		expectedMap := map[int]messageWithDataContent{
			1: newTestMessageWithDataContent(1),
		}

		require.Equal(t, expectedMap, q.messagesByOrder)
	})

	t.Run("OnAckReceived", func(t *testing.T) {
		receivedCount := 0

		q := newMessageQueue()
		q.OnAckReceived = func(count int) {
			receivedCount = count
		}

		err := q.AddMessages(newTestMessagesWithContent(1, 2, 3))
		require.NoError(t, err)

		err = q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 1,
			},
			{
				SeqNo: 3,
			},
		})

		require.NoError(t, err)
		require.Equal(t, 2, receivedCount)

		// Double ack
		err = q.AcksReceived([]rawtopicwriter.WriteAck{
			{
				SeqNo: 1,
			},
			{
				SeqNo: 3,
			},
		})

		require.Error(t, err)
		require.Equal(t, 0, receivedCount)
	})
}

func TestQueue_AckNotifiesOnlyMatchingWaiter(t *testing.T) {
	// An ack must wake only the waiters of the acked message, not every waiter of
	// the writer (regression guard against the previous shared-broadcast wakeup).
	q := newMessageQueue()
	require.NoError(t, q.AddMessages(newTestMessagesWithContent(1, 2)))

	wOther := &ackWaiter{done: make(chan struct{})}
	wTarget := &ackWaiter{done: make(chan struct{})}
	q.m.WithLock(func() {
		q.registerAckWaiterNeedLock(1, wOther)  // blocked on message order index 1
		q.registerAckWaiterNeedLock(2, wTarget) // blocked on message order index 2
	})

	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 2}}))

	select {
	case <-wTarget.done:
		// expected: matching waiter notified
	default:
		t.Fatal("waiter of the acked message was not notified")
	}

	select {
	case <-wOther.done:
		t.Fatal("unrelated waiter was notified (thundering herd)")
	default:
		// expected: unrelated waiter untouched
	}

	q.m.WithRLock(func() {
		_, stillWaiting := q.ackWaiters[1]
		require.True(t, stillWaiting, "unrelated waiter must stay registered")
		_, cleared := q.ackWaiters[2]
		require.False(t, cleared, "acked index must be removed from waiters")
	})
}

func TestQueue_WaitWokenOnOwnMessageAck(t *testing.T) {
	q := newMessageQueue()
	w1, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
	require.NoError(t, err)
	w2, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(2))
	require.NoError(t, err)

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	go func() { done1 <- q.Wait(t.Context(), w1) }()
	go func() { done2 <- q.Wait(t.Context(), w2) }()

	requireAckWaiterCount(t, &q, 2)

	// Ack only the second message: its waiter completes, the first keeps blocking.
	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 2}}))

	select {
	case err := <-done2:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("waiter of the acked message was not woken")
	}

	select {
	case <-done1:
		t.Fatal("waiter of the not-acked message returned early")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocking
	}

	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 1}}))
	select {
	case err := <-done1:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("waiter was not woken after its own message ack")
	}
}

func TestQueue_WaitCleansUpAckWaiter(t *testing.T) {
	t.Run("ContextCanceled", func(t *testing.T) {
		q := newMessageQueue()
		waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- q.Wait(ctx, waiter) }()

		requireAckWaiterCount(t, &q, 1)
		cancel()

		require.ErrorIs(t, waitQueueResult(t, done), context.Canceled)
		requireAckWaiterCount(t, &q, 0)
	})

	t.Run("QueueClosed", func(t *testing.T) {
		q := newMessageQueue()
		waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- q.Wait(t.Context(), waiter) }()

		requireAckWaiterCount(t, &q, 1)
		closeErr := errors.New("queue closed")
		require.NoError(t, q.Close(closeErr))

		require.ErrorIs(t, waitQueueResult(t, done), closeErr)
		requireAckWaiterCount(t, &q, 0)
	})
}

func TestQueue_WaitCleanupRaceWithAck(t *testing.T) {
	t.Run("ContextCanceled", func(t *testing.T) {
		for range 20 {
			q := newMessageQueue()
			waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			waitDone := make(chan error, 1)
			go func() { waitDone <- q.Wait(ctx, waiter) }()
			requireAckWaiterCount(t, &q, 1)

			start := make(chan struct{})
			cancelDone := make(chan struct{})
			ackDone := make(chan error, 1)
			go func() {
				<-start
				cancel()
				close(cancelDone)
			}()
			go func() {
				<-start
				ackDone <- q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 1}})
			}()

			close(start)
			<-cancelDone
			require.NoError(t, waitQueueResult(t, ackDone))
			waitErr := waitQueueResult(t, waitDone)
			if waitErr != nil {
				require.ErrorIs(t, waitErr, context.Canceled)
			}
			requireAckWaiterCount(t, &q, 0)
		}
	})

	t.Run("QueueClosed", func(t *testing.T) {
		for range 20 {
			q := newMessageQueue()
			waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
			require.NoError(t, err)

			waitDone := make(chan error, 1)
			go func() { waitDone <- q.Wait(t.Context(), waiter) }()
			requireAckWaiterCount(t, &q, 1)

			closeErr := errors.New("queue closed")
			start := make(chan struct{})
			closeDone := make(chan error, 1)
			ackDone := make(chan error, 1)
			go func() {
				<-start
				closeDone <- q.Close(closeErr)
			}()
			go func() {
				<-start
				ackDone <- q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 1}})
			}()

			close(start)
			require.NoError(t, waitQueueResult(t, closeDone))
			ackErr := waitQueueResult(t, ackDone)
			if ackErr != nil {
				require.ErrorIs(t, ackErr, errAckOnClosedMessageQueue)
			}
			waitErr := waitQueueResult(t, waitDone)
			if waitErr != nil {
				require.ErrorIs(t, waitErr, closeErr)
			}
			requireAckWaiterCount(t, &q, 0)
		}
	})
}

func requireAckWaiterCount(t *testing.T, q *messageQueue, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var registered int
		q.m.WithRLock(func() { registered = len(q.ackWaiters) })

		return registered == expected
	}, time.Second, time.Millisecond, "unexpected ack waiter count")
}

func waitQueueResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue operation")

		return nil
	}
}

func waitGetMessageStarted(q *messageQueue) {
	q.notifyNewMessages()
	for len(q.hasNewMessages) != 0 {
		runtime.Gosched()
	}
}

func getSeqNumbers(messages []messageWithDataContent) []int64 {
	res := make([]int64, 0, len(messages))
	for i := range messages {
		res = append(res, messages[i].SeqNo)
	}

	return res
}
