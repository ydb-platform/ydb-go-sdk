package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
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

		expected := map[int]*queuedMessage{
			1: {messageWithDataContent: newTestMessageWithDataContent(1)},
			2: {messageWithDataContent: newTestMessageWithDataContent(3)},
			3: {messageWithDataContent: newTestMessageWithDataContent(5)},
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
		q.messagesByOrder[maxInt] = &queuedMessage{messageWithDataContent: newTestMessageWithDataContent(1)}
		q.messagesByOrder[minInt] = &queuedMessage{messageWithDataContent: newTestMessageWithDataContent(3)}
		q.messagesByOrder[minInt+1] = &queuedMessage{messageWithDataContent: newTestMessageWithDataContent(5)}
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
		q.messagesByOrder[123] = &queuedMessage{}
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
		expectedMap := map[int]*queuedMessage{
			1: {messageWithDataContent: newTestMessageWithDataContent(1)},
			3: {messageWithDataContent: newTestMessageWithDataContent(5)},
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

		expectedMap := map[int]*queuedMessage{
			1: {messageWithDataContent: newTestMessageWithDataContent(1)},
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

	acked1 := waitForMessageAckChannel(t, &q, 1)
	acked2 := waitForMessageAckChannel(t, &q, 2)

	// Ack only the second message: its waiter completes, the first keeps blocking.
	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 2}}))

	require.NoError(t, waitQueueResult(t, done2))
	requireAckChannelOpen(t, acked1)
	select {
	case <-acked2:
	default:
		t.Fatal("acked message notification was not closed")
	}

	select {
	case <-done1:
		t.Fatal("waiter of the not-acked message returned early")
	default:
	}

	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 1}}))
	require.NoError(t, waitQueueResult(t, done1))
}

func TestQueue_WaitForBatch(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("ReverseAcks=%v", reverse), func(t *testing.T) {
			q := newMessageQueue()
			waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(10, 20))
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- q.Wait(t.Context(), waiter) }()
			acked := waitForMessageAckChannel(t, &q, 1)

			if reverse {
				require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 20}}))
				requireAckChannelOpen(t, acked)
			} else {
				require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 10}}))
				waitForMessageAckChannel(t, &q, 2)
			}
			select {
			case <-done:
				t.Fatal("batch waiter returned before all messages were acked")
			default:
			}

			lastSeqNo := int64(20)
			if reverse {
				lastSeqNo = 10
			}
			require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: lastSeqNo}}))
			require.NoError(t, waitQueueResult(t, done))
		})
	}
}

func TestQueue_WaitForBatchWithAckBeforeWait(t *testing.T) {
	q := newMessageQueue()
	waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(10, 20))
	require.NoError(t, err)

	// The first message is already absent when Wait starts; the second is still pending.
	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 10}}))

	ctx := &queueWaitContext{Context: t.Context(), waiting: make(empty.Chan, 1)}
	done := make(chan error, 1)
	go func() { done <- q.Wait(ctx, waiter) }()
	requireQueueWaitStarted(t, ctx)
	acked := waitForMessageAckChannel(t, &q, 2)
	requireAckChannelOpen(t, acked)

	select {
	case <-done:
		t.Fatal("batch waiter returned before the remaining message was acked")
	default:
	}

	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 20}}))
	require.NoError(t, waitQueueResult(t, done))
}

func TestQueue_WaitInterrupted(t *testing.T) {
	t.Run("ContextCanceled", func(t *testing.T) {
		q := newMessageQueue()
		waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- q.Wait(ctx, waiter) }()

		acked := waitForMessageAckChannel(t, &q, 1)
		cancel()

		require.ErrorIs(t, waitQueueResult(t, done), context.Canceled)
		requireAckChannelOpen(t, acked)

		// A canceled Wait does not cancel the message or another Wait/Flush.
		go func() { done <- q.WaitLastWritten(t.Context()) }()
		require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 1}}))
		require.NoError(t, waitQueueResult(t, done))
	})

	t.Run("QueueClosed", func(t *testing.T) {
		q := newMessageQueue()
		waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- q.Wait(t.Context(), waiter) }()

		waitForMessageAckChannel(t, &q, 1)
		closeErr := errors.New("queue closed")
		require.NoError(t, q.Close(closeErr))

		require.ErrorIs(t, waitQueueResult(t, done), closeErr)
	})
}

func TestQueue_WaitInterruptionRaceWithAck(t *testing.T) {
	t.Run("ContextCanceled", func(t *testing.T) {
		for range 20 {
			q := newMessageQueue()
			waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			waitDone := make(chan error, 1)
			go func() { waitDone <- q.Wait(ctx, waiter) }()
			waitForMessageAckChannel(t, &q, 1)

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
			require.Empty(t, q.messagesByOrder)
		}
	})

	t.Run("QueueClosed", func(t *testing.T) {
		for range 20 {
			q := newMessageQueue()
			waiter, err := q.AddMessagesWithWaiter(newTestMessagesWithContent(1))
			require.NoError(t, err)

			waitDone := make(chan error, 1)
			go func() { waitDone <- q.Wait(t.Context(), waiter) }()
			waitForMessageAckChannel(t, &q, 1)

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
		}
	})
}

func TestQueue_WaitLastWrittenSharesAck(t *testing.T) {
	q := newMessageQueue()
	require.NoError(t, q.AddMessages(newTestMessagesWithContent(1)))
	require.Nil(t, q.messagesByOrder[1].acked, "async writes must not allocate ack channels")

	done := make(chan error, 2)
	ctx1 := &queueWaitContext{Context: t.Context(), waiting: make(empty.Chan, 1)}
	go func() { done <- q.WaitLastWritten(ctx1) }()
	requireQueueWaitStarted(t, ctx1)
	acked := waitForMessageAckChannel(t, &q, 1)

	ctx, cancel := context.WithCancel(t.Context())
	ctx2 := &queueWaitContext{Context: ctx, waiting: make(empty.Chan, 1)}
	canceled := make(chan error, 1)
	go func() { canceled <- q.WaitLastWritten(ctx2) }()
	requireQueueWaitStarted(t, ctx2)
	cancel()
	require.ErrorIs(t, waitQueueResult(t, canceled), context.Canceled)
	q.m.WithRLock(func() { require.Equal(t, acked, q.messagesByOrder[1].acked) })
	requireAckChannelOpen(t, acked)

	ctx3 := &queueWaitContext{Context: t.Context(), waiting: make(empty.Chan, 1)}
	go func() { done <- q.WaitLastWritten(ctx3) }()
	requireQueueWaitStarted(t, ctx3)
	require.NoError(t, q.AcksReceived([]rawtopicwriter.WriteAck{{SeqNo: 1}}))
	require.NoError(t, waitQueueResult(t, done))
	require.NoError(t, waitQueueResult(t, done))
	// An ACK received before Wait starts must not be lost.
	require.NoError(t, q.WaitLastWritten(t.Context()))
}

const queueWaitTestTimeout = 5 * time.Second

// Done is called by Wait after obtaining the message's ack channel.
type queueWaitContext struct {
	context.Context //nolint:containedctx // Decorate Done to synchronize the test with Wait.

	waiting empty.Chan
}

func (c *queueWaitContext) Done() <-chan struct{} {
	select {
	case c.waiting <- empty.Struct{}:
	default:
	}

	return c.Context.Done()
}

func requireQueueWaitStarted(t *testing.T, ctx *queueWaitContext) {
	t.Helper()
	select {
	case <-ctx.waiting:
	case <-time.After(queueWaitTestTimeout):
		t.Fatal("Wait did not reach the ack notification select")
	}
}

func waitForMessageAckChannel(t *testing.T, q *messageQueue, index int) empty.Chan {
	t.Helper()
	var acked empty.Chan
	require.Eventually(t, func() bool {
		q.m.WithRLock(func() {
			if msg, ok := q.messagesByOrder[index]; ok {
				acked = msg.acked
			}
		})

		return acked != nil
	}, queueWaitTestTimeout, time.Millisecond, "Wait did not create the message ack channel")

	return acked
}

func requireAckChannelOpen(t *testing.T, acked empty.Chan) {
	t.Helper()
	select {
	case <-acked:
		t.Fatal("unacknowledged message notification was closed")
	default:
	}
}

func waitQueueResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(queueWaitTestTimeout):
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
