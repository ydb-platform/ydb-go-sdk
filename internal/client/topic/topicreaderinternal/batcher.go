package topicreaderinternal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var errBatcherPopConcurency = xerrors.Wrap(errors.New("ydb: batch pop concurency, internal state error"))

type batcher struct {
	popInFlight    int64
	closeErr       error
	hasNewMessages empty.Chan

	m xsync.Mutex

	forceIgnoreMinRestrictionsOnNextMessagesBatch bool
	closed                                        bool
	closeChan                                     empty.Chan
	messages                                      batcherMessagesMap
}

func newBatcher() *batcher {
	return &batcher{
		messages:       make(batcherMessagesMap),
		closeChan:      make(empty.Chan),
		hasNewMessages: make(empty.Chan, 1),
	}
}

func (b *batcher) Close(err error) error {
	b.m.Lock()
	defer b.m.Unlock()

	if b.closed {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: batch closed already: %w", err))
	}

	b.closed = true
	b.closeErr = err
	close(b.closeChan)
	return nil
}

func (b *batcher) PushBatches(batches ...*PublicBatch) error {
	b.m.Lock()
	defer b.m.Unlock()
	if b.closed {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: push batch to closed batcher :%w", b.closeErr))
	}

	for _, batch := range batches {
		if err := b.addNeedLock(batch.commitRange.partitionSession, newBatcherItemBatch(batch)); err != nil {
			return err
		}
	}

	return nil
}

func (b *batcher) PushRawMessage(session *partitionSession, m rawtopicreader.ServerMessage) error {
	b.m.Lock()
	defer b.m.Unlock()

	if b.closed {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: push raw message to closed batcher: %w", b.closeErr))
	}

	return b.addNeedLock(session, newBatcherItemRawMessage(m))
}

func (b *batcher) addNeedLock(session *partitionSession, item batcherMessageOrderItem) error {
	var currentItems batcherMessageOrderItems
	var ok bool
	var err error
	if currentItems, ok = b.messages[session]; ok {
		if currentItems, err = currentItems.Append(item); err != nil {
			return err
		}
	} else {
		currentItems = batcherMessageOrderItems{item}
	}

	b.messages[session] = currentItems

	b.notifyAboutNewMessages()

	return nil
}

type batcherGetOptions struct {
	MinCount        int
	MaxCount        int
	rawMessagesOnly bool
}

func (o batcherGetOptions) cutBatchItemsHead(items batcherMessageOrderItems) (
	head batcherMessageOrderItem,
	rest batcherMessageOrderItems,
	ok bool,
) {
	notFound := func() (batcherMessageOrderItem, batcherMessageOrderItems, bool) {
		return batcherMessageOrderItem{}, batcherMessageOrderItems{}, false
	}
	if len(items) == 0 {
		return notFound()
	}

	if items[0].IsBatch() {
		if o.rawMessagesOnly {
			return notFound()
		}

		batchHead, batchRest, ok := o.splitBatch(items[0].Batch)

		if !ok {
			return notFound()
		}

		head = newBatcherItemBatch(batchHead)
		rest = items.ReplaceHeadItem(newBatcherItemBatch(batchRest))
		return head, rest, true
	}

	return items[0], items[1:], true
}

func (o batcherGetOptions) splitBatch(batch *PublicBatch) (head, rest *PublicBatch, ok bool) {
	notFound := func() (*PublicBatch, *PublicBatch, bool) {
		return nil, nil, false
	}

	if len(batch.Messages) < o.MinCount {
		return notFound()
	}

	if o.MaxCount == 0 {
		return batch, nil, true
	}

	head, rest = batch.cutMessages(o.MaxCount)
	return head, rest, true
}

func (b *batcher) Pop(ctx context.Context, opts batcherGetOptions) (_ batcherMessageOrderItem, err error) {
	counter := atomic.AddInt64(&b.popInFlight, 1)
	defer atomic.AddInt64(&b.popInFlight, -1)

	if counter != 1 {
		return batcherMessageOrderItem{}, xerrors.WithStackTrace(errBatcherPopConcurency)
	}

	if err = ctx.Err(); err != nil {
		return batcherMessageOrderItem{}, err
	}

	for {
		var findRes batcherResultCandidate
		var closed bool

		b.m.WithLock(func() {
			closed = b.closed
			if closed {
				return
			}

			findRes = b.findNeedLock(opts)
			if findRes.Ok {
				b.applyNeedLock(findRes)
				return
			}
		})
		if closed {
			return batcherMessageOrderItem{},
				xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: try pop messages from closed batcher: %w", b.closeErr)))
		}
		if findRes.Ok {
			return findRes.Result, nil
		}

		// wait new messages for next iteration
		select {
		case <-b.hasNewMessages:
			// new iteration
		case <-b.closeChan:
			return batcherMessageOrderItem{},
				xerrors.WithStackTrace(
					fmt.Errorf(
						"ydb: batcher close while pop wait new messages: %w",
						b.closeErr,
					),
				)
		case <-ctx.Done():
			return batcherMessageOrderItem{}, ctx.Err()
		}
	}
}

func (b *batcher) notifyAboutNewMessages() {
	select {
	case b.hasNewMessages <- empty.Struct{}:
		// sent signal
	default:
		// signal already in progress
	}
}

type batcherResultCandidate struct {
	Key         *partitionSession
	Result      batcherMessageOrderItem
	Rest        batcherMessageOrderItems
	WaiterIndex int
	Ok          bool
}

func newBatcherResultCandidate(
	key *partitionSession,
	result batcherMessageOrderItem,
	rest batcherMessageOrderItems,
	ok bool,
) batcherResultCandidate {
	return batcherResultCandidate{
		Key:    key,
		Result: result,
		Rest:   rest,
		Ok:     ok,
	}
}

func (b *batcher) findNeedLock(filter batcherGetOptions) batcherResultCandidate {
	if len(b.messages) == 0 {
		return batcherResultCandidate{}
	}

	rawMessageOpts := batcherGetOptions{rawMessagesOnly: true}

	var batchResult batcherResultCandidate
	needBatchResult := true

	for k, items := range b.messages {
		head, rest, ok := rawMessageOpts.cutBatchItemsHead(items)
		if ok {
			return newBatcherResultCandidate(k, head, rest, true)
		}

		if needBatchResult {
			head, rest, ok = b.applyForceFlagToOptions(filter).cutBatchItemsHead(items)
			if !ok {
				continue
			}

			needBatchResult = false
			batchResult = newBatcherResultCandidate(k, head, rest, true)
		}
	}

	return batchResult
}

func (b *batcher) applyForceFlagToOptions(options batcherGetOptions) batcherGetOptions {
	if !b.forceIgnoreMinRestrictionsOnNextMessagesBatch {
		return options
	}

	res := options
	res.MinCount = 1
	return res
}

func (b *batcher) applyNeedLock(res batcherResultCandidate) {
	if res.Rest.IsEmpty() && res.WaiterIndex >= 0 {
		delete(b.messages, res.Key)
	} else {
		b.messages[res.Key] = res.Rest
	}

	if res.Result.IsBatch() {
		b.forceIgnoreMinRestrictionsOnNextMessagesBatch = false
	}
}

func (b *batcher) IgnoreMinRestrictionsOnNextPop() {
	b.m.Lock()
	defer b.m.Unlock()

	b.forceIgnoreMinRestrictionsOnNextMessagesBatch = true
	b.notifyAboutNewMessages()
}

type batcherMessagesMap map[*partitionSession]batcherMessageOrderItems

type batcherMessageOrderItems []batcherMessageOrderItem

func (items batcherMessageOrderItems) Append(item batcherMessageOrderItem) (batcherMessageOrderItems, error) {
	if len(items) == 0 {
		return append(items, item), nil
	}

	lastItem := &items[len(items)-1]
	if item.IsBatch() && lastItem.IsBatch() {
		if resBatch, err := lastItem.Batch.append(item.Batch); err == nil {
			lastItem.Batch = resBatch
		} else {
			return nil, err
		}
		return items, nil
	}

	return append(items, item), nil
}

func (items batcherMessageOrderItems) IsEmpty() bool {
	return len(items) == 0
}

func (items batcherMessageOrderItems) ReplaceHeadItem(item batcherMessageOrderItem) batcherMessageOrderItems {
	if item.IsEmpty() {
		return items[1:]
	}

	res := make(batcherMessageOrderItems, len(items))
	res[0] = item
	copy(res[1:], items[1:])
	return res
}

type batcherMessageOrderItem struct {
	Batch      *PublicBatch
	RawMessage rawtopicreader.ServerMessage
}

func newBatcherItemBatch(b *PublicBatch) batcherMessageOrderItem {
	return batcherMessageOrderItem{Batch: b}
}

func newBatcherItemRawMessage(b rawtopicreader.ServerMessage) batcherMessageOrderItem {
	return batcherMessageOrderItem{RawMessage: b}
}

func (item *batcherMessageOrderItem) IsBatch() bool {
	return !item.Batch.isEmpty()
}

func (item *batcherMessageOrderItem) IsRawMessage() bool {
	return item.RawMessage != nil
}

func (item *batcherMessageOrderItem) IsEmpty() bool {
	return item.RawMessage == nil && item.Batch.isEmpty()
}
