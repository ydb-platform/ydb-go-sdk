package topicproducer

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/spaolacci/murmur3"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type messagePtr *xlist.Element[Message]

type worker struct {
	ctx  context.Context //nolint:containedctx
	err  error
	stop context.CancelFunc

	writers               map[int64]*writerWrapper
	idleWritersSupervisor *idleWritersSupervisor
	cfg                   *ProducerConfig
	mu                    xsync.Mutex
	messagesSemaphore     *xsync.SoftWeightedSemaphore

	partitionChooser PartitionChooser

	inFlightMessages      xlist.List[Message]
	inFlightMessagesIndex map[int64]xlist.List[messagePtr]
	pendingMessagesIndex  map[int64]xlist.List[messagePtr]
	messagesToResendIndex map[int64]xlist.List[messagePtr]

	topicDescriber TopicDescriber

	wakeupChan empty.Chan
	shutdown   empty.Chan

	partitions map[int64]*PartitionInfo
	initDone   chan struct{}

	isAutoPartitioningEnabled bool
	background                *background.Worker
}

func newWorker(
	ctx context.Context,
	stop context.CancelFunc,
	shutdown empty.Chan,
	topicDescriber TopicDescriber,
	background *background.Worker,
	cfg *ProducerConfig,
) *worker {
	if cfg.writersFactory == nil {
		cfg.writersFactory = newBaseWritersFactory()
	}

	if cfg.SubSessionIdleTimeout == 0 {
		cfg.SubSessionIdleTimeout = defaultWriterIdleTimeout
	}

	if cfg.MaxQueueLen == 0 {
		cfg.MaxQueueLen = defaultInFlightMessagesBufferSize
	}

	w := &worker{
		writers:               make(map[int64]*writerWrapper),
		inFlightMessages:      xlist.New[Message](),
		cfg:                   cfg,
		wakeupChan:            make(empty.Chan, 1),
		shutdown:              shutdown,
		topicDescriber:        topicDescriber,
		ctx:                   ctx,
		stop:                  stop,
		partitions:            make(map[int64]*PartitionInfo),
		initDone:              make(chan struct{}),
		messagesSemaphore:     xsync.NewSoftWeightedSemaphore(int64(cfg.MaxQueueLen) / 2),
		inFlightMessagesIndex: make(map[int64]xlist.List[messagePtr]),
		messagesToResendIndex: make(map[int64]xlist.List[messagePtr]),
		pendingMessagesIndex:  make(map[int64]xlist.List[messagePtr]),
		background:            background,
	}

	if cfg.PartitioningKeyHasher == nil {
		cfg.PartitioningKeyHasher = w.getDefaultKeyHasher()
	}

	w.idleWritersSupervisor = newIdleWritersSupervisor(ctx, w, cfg.SubSessionIdleTimeout)
	background.Start("idle writers supervisor", func(ctx context.Context) {
		w.idleWritersSupervisor.run()
	})

	return w
}

func isOperationErrorOverloaded(err error) bool {
	return xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED)
}

func (w *worker) getDefaultKeyHasher() KeyHasher {
	return func(key string) string {
		// Same as C++ TProducerSettings::DefaultPartitioningKeyHasher:
		// MurmurHash64 with seed 0, result as 8 bytes in big-endian (network byte order)
		lo := murmur3.Sum64([]byte(key))
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, lo)

		return string(out)
	}
}

func (w *worker) init() (err error) {
	defer close(w.initDone)

	describeResult, err := w.topicDescriber(w.ctx, w.cfg.Topic())
	if err != nil {
		return err
	}

	for _, partition := range describeResult.Partitions {
		var parentID *int64
		if len(partition.ParentPartitionIDs) > 0 {
			parentID = &partition.ParentPartitionIDs[0]
		}

		w.partitions[partition.PartitionID] = &PartitionInfo{
			ID:        partition.PartitionID,
			ParentID:  parentID,
			Children:  partition.ChildPartitionIDs,
			FromBound: partition.FromBound,
			ToBound:   partition.ToBound,
		}
	}

	w.isAutoPartitioningEnabled = describeResult.PartitionSettings.AutoPartitioningSettings.AutoPartitioningStrategy !=
		topictypes.AutoPartitioningStrategyDisabled

	switch w.cfg.PartitionChooserStrategy {
	case PartitionChooserStrategyBound:
		w.partitionChooser, err = newBoundPartitionChooser(w.cfg, w.partitions)
		if err != nil {
			return err
		}
	case PartitionChooserStrategyHash:
		partitionIDs := make([]int64, 0, len(w.partitions))
		for id := range w.partitions {
			partitionIDs = append(partitionIDs, id)
		}

		w.partitionChooser = newHashPartitionChooser(w.cfg, partitionIDs)
	}

	return nil
}

func (w *worker) choosePartition(msg Message) (partitionID int64, err error) {
	switch {
	case msg.PartitionID != 0:
		return msg.PartitionID, nil
	case msg.Key != "":
		partitionID, err = w.partitionChooser.ChoosePartition(msg.Key)
		if err != nil {
			return
		}
	case w.cfg.CustomChoosePartitionFunc != nil:
		partitionID, err = w.cfg.CustomChoosePartitionFunc(msg)
		if err != nil {
			return
		}
	default:
		partitionID, err = w.partitionChooser.ChoosePartition(w.cfg.ProducerIDPrefix)
		if err != nil {
			return
		}
	}

	return partitionID, nil
}

// no concurrent safe
func (w *worker) addToInFlightMessagesIndex(newElement messagePtr, toPartition int64, toFront bool) {
	list, ok := w.inFlightMessagesIndex[toPartition]
	if !ok {
		list = xlist.New[messagePtr]()
		w.inFlightMessagesIndex[toPartition] = list
	}

	if toFront {
		list.PushFront(newElement)
	} else {
		list.PushBack(newElement)
	}
}

// no concurrent safe
func (w *worker) addToPendingMessagesIndex(newElement messagePtr, toPartition int64, toFront bool) {
	list, ok := w.pendingMessagesIndex[toPartition]
	if !ok {
		list = xlist.New[messagePtr]()
		w.pendingMessagesIndex[toPartition] = list
	}

	if toFront {
		list.PushFront(newElement)
	} else {
		list.PushBack(newElement)
	}
}

// no concurrent safe
func (w *worker) addToMessagesToResendIndex(newElement messagePtr, toPartition int64, toFront bool) {
	list, ok := w.messagesToResendIndex[toPartition]
	if !ok {
		list = xlist.New[messagePtr]()
		w.messagesToResendIndex[toPartition] = list
	}

	if toFront {
		list.PushFront(newElement)
	} else {
		list.PushBack(newElement)
	}
}

func (w *worker) pushMessage(ctx context.Context, msg Message) (err error) {
	var autoSetSeqNo bool
	w.mu.WithLock(func() {
		autoSetSeqNo = w.cfg.AutoSetSeqNo
	})

	if !autoSetSeqNo && msg.SeqNo == 0 {
		return ErrNoSeqNo
	}

	if err := w.messagesSemaphore.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("ydb: can not add message due to queue len overflow: %w", err)
	}

	w.mu.WithLock(func() {
		msg.PartitionID, err = w.choosePartition(msg)
		if err != nil {
			return
		}

		newElement := w.inFlightMessages.PushBack(msg)
		w.addToInFlightMessagesIndex(newElement, msg.PartitionID, false)
		w.addToPendingMessagesIndex(newElement, msg.PartitionID, false)
		w.wakeup()
	})

	return nil
}

func (w *worker) removeWriter(partitionID int64) {
	var toClose writer

	w.mu.WithLock(func() {
		wr, ok := w.writers[partitionID]
		if !ok {
			return
		}

		toClose = wr
		delete(w.writers, partitionID)
	})

	if toClose == nil {
		return
	}

	if err := toClose.Close(w.ctx); err != nil {
		w.mu.WithLock(func() {
			w.err = err
			w.stop()
		})

		return
	}
}

func (w *worker) closeWriters(ctx context.Context) (finalErr error) {
	w.mu.WithLock(func() {
		for _, wr := range w.writers {
			if err := wr.Close(ctx); err != nil {
				finalErr = err

				return
			}
		}
	})

	return
}

func (w *worker) getProducerID(partitionID int64) string {
	return fmt.Sprintf("%s_%d", w.cfg.ProducerIDPrefix, partitionID)
}

func (w *worker) createWriter(partitionID int64) (writer, error) {
	withCustomCheckRetryErrorFunction := func(
		callback topic.PublicCheckErrorRetryFunction,
	) topicwriterinternal.PublicWriterOption {
		return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
			cfg.RetrySettings.CheckError = callback
		}
	}

	var (
		writerCfg = w.cfg.WriterReconnectorConfig
		opts      = []topicwriterinternal.PublicWriterOption{
			topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithPartitionID(partitionID)),
			topicwriterinternal.WithProducerID(w.getProducerID(partitionID)),
			topicwriterinternal.WithOnAckReceivedCallback(func(seqNo int64) {
				w.mu.WithLock(func() {
					w.onAckReceived(partitionID, seqNo)
				})
			}),
			withCustomCheckRetryErrorFunction(func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
				if isOperationErrorOverloaded(args.Error) {
					if err := w.onPartitionSplit(partitionID); err != nil {
						w.mu.WithLock(func() {
							w.err = err
							w.stop()
						})
					}

					return topic.PublicRetryDecisionStop
				}

				return w.cfg.RetrySettings.CheckError(args)
			}),
			topicwriterinternal.WithMaxQueueLen(w.cfg.MaxQueueLen / 2),
		}
	)

	for _, opt := range opts {
		opt(&writerCfg)
	}

	wr, err := w.cfg.writersFactory.Create(writerCfg)
	if err != nil {
		return nil, err
	}

	return wr, nil
}

// no concurrent safe
func (w *worker) onAckReceived(partitionID, seqNo int64) {
	indexChain, ok := w.inFlightMessagesIndex[partitionID]
	if !ok {
		return
	}

	message := indexChain.Front()
	if message.Value.Value.SeqNo != 0 && message.Value.Value.SeqNo != seqNo {
		panic("seqNo mismatch")
	}

	message.Value.Value.ackReceived = true

	indexChain.Remove(message)
	if indexChain.Len() == 0 {
		delete(w.inFlightMessagesIndex, partitionID)
		_, ok := w.writers[partitionID]
		if ok {
			w.idleWritersSupervisor.add(partitionID)
		}
	}

	w.releaseInFlightMessages()
	if len(w.pendingMessagesIndex) > 0 {
		w.wakeup()
	}
}

func (w *worker) releaseInFlightMessages() {
	for w.inFlightMessages.Len() > 0 {
		front := w.inFlightMessages.Front()
		if !front.Value.ackReceived {
			return
		}

		if front.Value.onAckCallback != nil {
			front.Value.onAckCallback()
		}

		w.inFlightMessages.Remove(front)
		w.messagesSemaphore.Release(1)
	}
}

func (w *worker) getSplittedPartitionAncestors(
	describeResult *topictypes.TopicDescription,
	partitionID int64,
) []int64 {
	partitionToParent := make(map[int64]int64)
	for _, partition := range describeResult.Partitions {
		if len(partition.ParentPartitionIDs) == 0 {
			continue
		}
		partitionToParent[partition.PartitionID] = partition.ParentPartitionIDs[0]
	}

	var (
		ancestors          = []int64{partitionID}
		currentPartitionID = partitionID
	)

	for {
		parentID, ok := partitionToParent[currentPartitionID]
		if !ok {
			break
		}

		ancestors = append(ancestors, parentID)
		currentPartitionID = parentID
	}

	return ancestors
}

func (w *worker) addNewPartitions(describeResult *topictypes.TopicDescription, splittedPartitionID int64) {
	for _, partition := range describeResult.Partitions {
		if len(partition.ParentPartitionIDs) > 0 && partition.ParentPartitionIDs[0] == splittedPartitionID {
			w.partitions[partition.PartitionID] = &PartitionInfo{
				ID:        partition.PartitionID,
				ParentID:  &splittedPartitionID,
				Children:  partition.ChildPartitionIDs,
				FromBound: partition.FromBound,
				ToBound:   partition.ToBound,
				Locked:    true,
			}
		}
	}
}

func (w *worker) getSplittedPartitionChildren(describeResult *topictypes.TopicDescription, partitionID int64) []int64 {
	for _, partition := range describeResult.Partitions {
		if partition.PartitionID == partitionID {
			return partition.ChildPartitionIDs
		}
	}

	return nil
}

func (w *worker) rechoosePartition(msg *Message) (err error) {
	msg.PartitionID = 0
	msg.PartitionID, err = w.choosePartition(*msg)

	return err
}

func (w *worker) scheduleResendMessages(partitionID, maxSeqNo int64) (err error) {
	inFlightIndexChain, ok := w.inFlightMessagesIndex[partitionID]
	if !ok {
		return nil
	}

	var (
		inFlightMessagesToAdd []messagePtr
		pendingMessagesToAdd  []messagePtr
		messagesToResendToAdd []messagePtr
	)

	for inFlightIndexChain.Len() > 0 {
		iter := inFlightIndexChain.Front()

		msg := iter.Value.Value
		if msg.SeqNo < maxSeqNo {
			if msg.ackReceived {
				continue
			}

			w.onAckReceived(partitionID, msg.SeqNo)

			continue
		}

		if err := w.rechoosePartition(&msg); err != nil {
			return err
		}

		iter.Value.Value.PartitionID = msg.PartitionID
		inFlightMessagesToAdd = append(inFlightMessagesToAdd, iter.Value)
		if iter.Value.Value.sent {
			messagesToResendToAdd = append(messagesToResendToAdd, iter.Value)
		} else {
			pendingMessagesToAdd = append(pendingMessagesToAdd, iter.Value)
		}
		inFlightIndexChain.Remove(iter)
	}

	for i := len(inFlightMessagesToAdd) - 1; i >= 0; i-- {
		w.addToInFlightMessagesIndex(inFlightMessagesToAdd[i], inFlightMessagesToAdd[i].Value.PartitionID, true)
	}
	for i := len(pendingMessagesToAdd) - 1; i >= 0; i-- {
		w.addToPendingMessagesIndex(pendingMessagesToAdd[i], pendingMessagesToAdd[i].Value.PartitionID, true)
	}
	for i := len(messagesToResendToAdd) - 1; i >= 0; i-- {
		w.addToMessagesToResendIndex(messagesToResendToAdd[i], messagesToResendToAdd[i].Value.PartitionID, true)
	}

	delete(w.inFlightMessagesIndex, partitionID)
	delete(w.pendingMessagesIndex, partitionID)

	if len(w.pendingMessagesIndex) > 0 || len(w.messagesToResendIndex) > 0 {
		w.wakeup()
	}

	return nil
}

func (w *worker) getMaxSeqNo(ancestors []int64) (maxSeqNo int64, err error) {
	var (
		mu       xsync.Mutex
		errGroup errgroup.Group
	)

	errGroup.SetLimit(10)

	for _, ancestor := range ancestors {
		errGroup.Go(func() error {
			writerCfg := w.cfg.WriterReconnectorConfig
			topicwriterinternal.WithProducerID(w.getProducerID(ancestor))(&writerCfg)

			writer, err := w.cfg.writersFactory.Create(writerCfg)
			if err != nil {
				return err
			}

			initInfo, err := writer.WaitInit(w.ctx)
			if err != nil {
				return err
			}

			mu.WithLock(func() {
				maxSeqNo = max(maxSeqNo, initInfo.LastSeqNum)
			})

			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return 0, err
	}

	return maxSeqNo, nil
}

func (w *worker) onPartitionSplit(partitionID int64) (resultErr error) {
	describeResult, err := w.topicDescriber(w.ctx, w.cfg.Topic())
	if err != nil {
		return err
	}

	w.mu.WithLock(func() {
		partition := w.partitions[partitionID]
		partition.Locked = true
		partition.Children = w.getSplittedPartitionChildren(&describeResult, partitionID)
		w.addNewPartitions(&describeResult, partitionID)
		for _, child := range partition.Children {
			childPartition := w.partitions[child]
			w.partitionChooser.AddNewPartition(child, childPartition.FromBound, childPartition.ToBound)
		}
		w.partitionChooser.RemovePartition(partitionID)
	})

	ancestors := w.getSplittedPartitionAncestors(&describeResult, partitionID)
	maxSeqNo, err := w.getMaxSeqNo(ancestors)
	if err != nil {
		return err
	}

	w.mu.WithLock(func() {
		partition := w.partitions[partitionID]
		partition.Locked = false
		err = w.scheduleResendMessages(partitionID, maxSeqNo)
		if err != nil {
			w.err = err
			w.stop()

			return
		}

		for _, child := range partition.Children {
			w.partitions[child].Locked = false
		}
	})

	return nil
}

func (w *worker) wakeup() {
	select {
	case w.wakeupChan <- empty.Struct{}:
	default:
	}
}

func (w *worker) getWriter(partitionID int64) (*writerWrapper, error) {
	wr, ok := w.writers[partitionID]
	if !ok {
		partitionWriter, err := w.createWriter(partitionID)
		if err != nil {
			return nil, err
		}

		wrapper := &writerWrapper{
			writer: partitionWriter,
		}
		w.writers[partitionID] = wrapper

		w.background.Start(fmt.Sprintf("wait init for partition %d", partitionID), func(ctx context.Context) {
			_, err = partitionWriter.WaitInit(w.ctx)
			w.mu.WithLock(func() {
				if err != nil {
					w.err = err
					w.stop()

					return
				}
				wrapper.initDone = true

				w.wakeup()
			})
		})

		w.idleWritersSupervisor.remove(partitionID)

		return wrapper, nil
	}

	w.idleWritersSupervisor.remove(partitionID)

	return wr, nil
}

func (w *worker) getResultErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.err
}

func (w *worker) flush(ctx context.Context) error {
	waitCh := make(empty.Chan, 1)

	w.mu.WithLock(func() {
		if w.inFlightMessages.Len() == 0 {
			waitCh <- empty.Struct{}

			return
		}

		lastInFlightMessage := w.inFlightMessages.Back()
		prevAckCallback := lastInFlightMessage.Value.onAckCallback
		lastInFlightMessage.Value.onAckCallback = func() {
			if prevAckCallback != nil {
				prevAckCallback()
			}

			waitCh <- empty.Struct{}
		}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		return w.getResultErr()
	case <-waitCh:
		return nil
	}
}

func (w *worker) step() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	iterateThroughMessagesIndex := func(index map[int64]xlist.List[messagePtr], stopFunc func(msg messagePtr) bool) error {
		var partitionsToRemove []int64

		for partitionID, list := range index {
			for iter := list.Front(); iter != nil; iter = iter.Next() {
				msg := iter.Value.Value

				if w.partitions[msg.PartitionID].Locked || stopFunc(iter.Value) {
					break
				}

				wr, err := w.getWriter(msg.PartitionID)
				if err != nil {
					return fmt.Errorf("failed to get writer: %w", err)
				}

				if !wr.initDone {
					break
				}

				err = wr.Write(w.ctx, []topicwriterinternal.PublicMessage{msg.PublicMessage})
				if err != nil {
					return fmt.Errorf("failed to write message: %w", err)
				}

				list.Remove(iter)
				iter.Value.Value.sent = true
			}

			if list.Len() == 0 {
				partitionsToRemove = append(partitionsToRemove, partitionID)
			}
		}

		for _, partitionID := range partitionsToRemove {
			delete(index, partitionID)
		}

		return nil
	}

	if err := iterateThroughMessagesIndex(
		w.messagesToResendIndex,
		func(msg messagePtr) bool { return false },
	); err != nil {
		return err
	}

	return iterateThroughMessagesIndex(
		w.pendingMessagesIndex,
		func(msg messagePtr) bool {
			_, ok := w.messagesToResendIndex[msg.Value.PartitionID]

			return ok
		},
	)
}

func (w *worker) waitInitDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.initDone:
		return w.getResultErr()
	}
}

func (w *worker) run() {
	defer close(w.shutdown)

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.wakeupChan:
		}

		if err := w.step(); err != nil {
			w.mu.WithLock(func() {
				w.err = err
				w.stop()
			})

			return
		}
	}
}
