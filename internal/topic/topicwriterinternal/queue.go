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
)

const (
	intSize = 32 << (^uint(0) >> 63) // copy from math package for use in go <= 1.16
	maxInt  = 1<<(intSize-1) - 1     // copy from math package for use in go <= 1.16
	minInt  = -1 << (intSize - 1)    // copy from math package for use in go <= 1.16

	minPositiveIndexWhichOrderLessThenNegative = maxInt / 2
)

type messageQueue struct {
	hasNewMessages empty.Chan
	closedErr      error

	m                xsync.Mutex
	closed           bool
	closedChan       empty.Chan
	lastWrittenIndex int
	lastSentIndex    int

	messagesByOrder map[int]messageWithDataContent
	seqNoToOrderId  map[int64]orderIDsFIFO
}

func newMessageQueue() messageQueue {
	return messageQueue{
		messagesByOrder: make(map[int]messageWithDataContent),
		seqNoToOrderId:  make(map[int64]orderIDsFIFO),
		hasNewMessages:  make(empty.Chan, 1),
		closedChan:      make(empty.Chan),
	}
}

func (q *messageQueue) AddMessages(messages *messageWithDataContentSlice) error {
	defer putContentMessagesSlice(messages)

	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return xerrors.WithStackTrace(errAddMessageToClosedQueue)
	}

	for i := range messages.m {
		q.addMessage(messages.m[i])
	}

	q.notifyNewMessages()

	return nil
}

func (q *messageQueue) notifyNewMessages() {
	select {
	case q.hasNewMessages <- empty.Struct{}:
		// pass
	default:
	}
}

func (q *messageQueue) addMessage(mess messageWithDataContent) {
	q.lastWrittenIndex++

	if q.lastWrittenIndex == minInt {
		q.ensureNoSmallIntIndexes()
	}

	if _, ok := q.messagesByOrder[q.lastWrittenIndex]; ok {
		panic(fmt.Errorf("ydb: bad internal state os message queue - already exists with index: %v", q.lastWrittenIndex))
	}

	q.messagesByOrder[q.lastWrittenIndex] = mess

	// append orderID
	orderIDs := q.seqNoToOrderId[mess.SeqNo]
	orderIDs.Push(q.lastWrittenIndex)
	q.seqNoToOrderId[mess.SeqNo] = orderIDs
}

func (q *messageQueue) AcksReceived(acks []rawtopicwriter.WriteAck) {
	q.m.Lock()
	defer q.m.Unlock()

	for i := range acks {
		q.ackReceived(acks[i].SeqNo)
	}
}

func (q *messageQueue) ackReceived(seqNo int64) {
	orderIDs := q.seqNoToOrderId[seqNo]
	if orderIDs.Len == 0 {
		return
	}

	orderID := orderIDs.Pop()
	if orderIDs.Len == 0 {
		delete(q.seqNoToOrderId, seqNo)
	} else {
		q.seqNoToOrderId[seqNo] = orderIDs
	}

	delete(q.messagesByOrder, orderID)
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

type orderIDsFIFO struct {
	FirstOrderID int
	Len          int
	Tail         []int
}

func newOrderIDsFIFO(ids ...int) (res orderIDsFIFO) {
	res.Len = len(ids)
	if res.Len == 0 {
		return
	}
	res.FirstOrderID = ids[0]
	if res.Len > 1 {
		res.Tail = ids[1:]
	}
	return res
}

func (s *orderIDsFIFO) Pop() int {
	if s.Len == 0 {
		panic("ydb: internal state error - pop from empty queue order id storage")
	}

	s.Len--
	res := s.FirstOrderID
	if len(s.Tail) > 0 {
		s.FirstOrderID = s.Tail[0]
		s.Tail = s.Tail[1:]
	}
	return res
}

func (s *orderIDsFIFO) Push(v int) {
	if s.Len > 0 {
		s.Tail = append(s.Tail, v)
	} else {
		s.FirstOrderID = v
	}
	s.Len++
}
