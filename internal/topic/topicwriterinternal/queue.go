// nolint
package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var (
	errAddMessageToClosedQueue   = xerrors.Wrap(errors.New("ydb: add message to closed message queue"))
	errCloseClosedMessageQueue   = xerrors.Wrap(errors.New("ydb: close closed message queue"))
	errGetMessageFromClosedQueue = xerrors.Wrap(errors.New("ydb: get message from closed message queue"))
	errAddUnorderedMessages      = xerrors.Wrap(errors.New("ydb: add unordered messages"))
	errAckUnexpectedMessage      = xerrors.Wrap(errors.New("ydb: ack unexpected message"))
)

const (
	intSize = 32 << (^uint(0) >> 63) // copy from math package for use in go <= 1.16
	maxInt  = 1<<(intSize-1) - 1     // copy from math package for use in go <= 1.16
	minInt  = -1 << (intSize - 1)    // copy from math package for use in go <= 1.16

	minPositiveIndexWhichOrderLessThenNegative = maxInt / 2
)

type messageQueue struct {
	hasNewMessages    empty.Chan
	closedErr         error
	acksReceivedEvent xsync.EventBroadcast

	m                xsync.RWMutex
	closed           bool
	closedChan       empty.Chan
	lastWrittenIndex int
	lastSentIndex    int
	lastSeqNo        int64

	messagesByOrder map[int]messageWithDataContent
	seqNoToOrderId  map[int64]int
}

func newMessageQueue() messageQueue {
	return messageQueue{
		messagesByOrder: make(map[int]messageWithDataContent),
		seqNoToOrderId:  make(map[int64]int),
		hasNewMessages:  make(empty.Chan, 1),
		closedChan:      make(empty.Chan),
		lastSeqNo:       -1,
	}
}

func (q *messageQueue) AddMessages(messages *messageWithDataContentSlice) error {
	_, err := q.addMessages(messages, false)
	return err
}

func (q *messageQueue) AddMessagesWithWaiter(messages *messageWithDataContentSlice) (waiter *MessageQueueAckWaiter, err error) {
	return q.addMessages(messages, true)
}

func (q *messageQueue) addMessages(messages *messageWithDataContentSlice, needWaiter bool) (waiter *MessageQueueAckWaiter, err error) {
	defer putContentMessagesSlice(messages)

	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: add message to closed message queue: %w", q.closedErr))
	}

	if err := q.checkNewMessagesBeforeAddNeedLock(messages); err != nil {
		return nil, err
	}

	if needWaiter {
		waiter = newMessageQueueAckWaiter()
	}

	for i := range messages.m {
		messageIndex := q.addMessageNeedLock(messages.m[i])
		if needWaiter {
			waiter.AddWaitIndex(messageIndex)
		}
	}

	q.notifyNewMessages()

	return waiter, nil
}

func (q *messageQueue) notifyNewMessages() {
	select {
	case q.hasNewMessages <- empty.Struct{}:
		// pass
	default:
	}
}

func (q *messageQueue) checkNewMessagesBeforeAddNeedLock(messages *messageWithDataContentSlice) error {
	if len(messages.m) == 0 {
		return nil
	}

	checkedSeqNo := q.lastSeqNo
	for _, m := range messages.m {
		if m.SeqNo <= checkedSeqNo {
			return xerrors.WithStackTrace(errAddUnorderedMessages)
		}
		checkedSeqNo = m.SeqNo
	}

	return nil
}

func (q *messageQueue) addMessageNeedLock(mess messageWithDataContent) (messageIndex int) {
	q.lastWrittenIndex++
	messageIndex = q.lastWrittenIndex

	if messageIndex == minInt {
		q.ensureNoSmallIntIndexes()
	}

	if _, ok := q.messagesByOrder[messageIndex]; ok {
		panic(fmt.Errorf("ydb: bad internal state os message queue - already exists with index: %v", messageIndex))
	}

	q.messagesByOrder[messageIndex] = mess
	q.seqNoToOrderId[mess.SeqNo] = messageIndex
	q.lastSeqNo = mess.SeqNo
	return messageIndex
}

func (q *messageQueue) AcksReceived(acks []rawtopicwriter.WriteAck) error {
	q.m.Lock()
	defer q.m.Unlock()

	for i := range acks {
		if err := q.ackReceivedNeedLock(acks[i].SeqNo); err != nil {
			return err
		}
	}

	q.acksReceivedEvent.Broadcast()
	return nil
}

func (q *messageQueue) ackReceivedNeedLock(seqNo int64) error {
	orderID, ok := q.seqNoToOrderId[seqNo]
	if !ok {
		return xerrors.WithStackTrace(errAckUnexpectedMessage)
	}

	delete(q.seqNoToOrderId, seqNo)
	delete(q.messagesByOrder, orderID)
	return nil
}

func (q *messageQueue) Close(err error) error {
	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return xerrors.WithStackTrace(errCloseClosedMessageQueue)
	}
	q.closed = true
	q.closedErr = err
	close(q.closedChan)
	return nil
}

func (q *messageQueue) ensureNoSmallIntIndexes() {
	for k := range q.messagesByOrder {
		if k >= 0 && k < minPositiveIndexWhichOrderLessThenNegative {
			panic("ydb: message queue has bad state - overflow or has very old element")
		}
	}
}

func (q *messageQueue) GetMessagesForSend(ctx context.Context) (*messageWithDataContentSlice, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var closed bool
	q.m.WithLock(func() {
		closed = q.closed
	})
	if closed {
		return nil, xerrors.WithStackTrace(errGetMessageFromClosedQueue)
	}

	for {
		res := q.getMessagesForSendWithLock()
		if res != nil {
			return res, nil
		}

		select {
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		case <-q.hasNewMessages:
			// pass
		case <-q.closedChan:
			return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: message queue closed with: %w", q.closedErr))
		}
	}
}

func (q *messageQueue) ResetSentProgress() {
	q.m.Lock()
	defer q.m.Unlock()

	minKey := q.lastWrittenIndex
	for k := range q.messagesByOrder {
		if isFirstCycledIndexLess(k, minKey) {
			minKey = k
		}
	}

	q.lastSentIndex = minKey - 1
	q.notifyNewMessages()
}

func (q *messageQueue) getMessagesForSendWithLock() *messageWithDataContentSlice {
	q.m.Lock()
	defer q.m.Unlock()

	if q.lastWrittenIndex == q.lastSentIndex {
		return nil
	}

	res := newContentMessagesSlice()
	for {
		// use  "!=" instead of  "<" - for work with negative indexes after overflow
		if q.lastWrittenIndex == q.lastSentIndex {
			break
		}
		q.lastSentIndex++

		if msg, ok := q.messagesByOrder[q.lastSentIndex]; ok {
			res.m = append(res.m, msg)
		} else {
			// msg may be unexisted if it already has ack from server
			// pass
		}
	}
	if len(res.m) == 0 {
		putContentMessagesSlice(res)
		res = nil
	}
	return res
}

func (q *messageQueue) Wait(ctx context.Context, waiter *MessageQueueAckWaiter) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctxDone := ctx.Done()
	for {
		ackReceived := q.acksReceivedEvent.Waiter()

		hasWaited := false
		q.m.WithRLock(func() {
			for _, k := range waiter.sequenseNumbers {
				if _, ok := q.messagesByOrder[k]; ok {
					hasWaited = true
					return
				}
			}
		})

		if !hasWaited {
			return nil
		}

		select {
		case <-ctxDone:
			return ctx.Err()
		case <-q.closedChan:
			return q.closedErr
		case <-ackReceived.Done():
			// pass next iteration
		}
	}
}

type MessageQueueAckWaiter struct {
	sequenseNumbers []int
}

func (m *MessageQueueAckWaiter) init() {
}

func (m *MessageQueueAckWaiter) AddWaitIndex(index int) {
	m.sequenseNumbers = append(m.sequenseNumbers, index)
}

func (m *MessageQueueAckWaiter) reset() {
	m.sequenseNumbers = m.sequenseNumbers[:0]
}

// sortMessageQueueIndexes deprecated
func sortMessageQueueIndexes(keys []int) {
	sort.Ints(keys)
	// check index overflow
	if len(keys) > 0 && keys[0] < 0 && keys[len(keys)-1] > 0 {
		sort.Slice(keys, func(i, k int) bool {
			return isFirstCycledIndexLess(keys[i], keys[k])
		})
	}
}

func isFirstCycledIndexLess(first, second int) bool {
	switch {
	case first > minPositiveIndexWhichOrderLessThenNegative && second < 0:
		return true
	case first < 0 && second > minPositiveIndexWhichOrderLessThenNegative:
		return false
	default:
		return first < second
	}
}
