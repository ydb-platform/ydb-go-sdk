package topicmultiwriter

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

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

type message struct {
	topicwriterinternal.PublicMessage

	onAckCallback func()
	ackReceived   bool
	sent          bool
}

type messagePtr *xlist.Element[message]

type worker struct {
	ctx  context.Context //nolint:containedctx
	err  error
	stop context.CancelFunc

	writers               map[int64]*writerWrapper
	idleWritersSupervisor *idleWritersSupervisor
	cfg                   *MultiWriterConfig
	mu                    xsync.Mutex
	messagesSema          empty.Chan

	partitionChooser PartitionChooser

	inFlightMessages      xlist.List[message]
	inFlightMessagesIndex map[int64]xlist.List[messagePtr]
	pendingMessagesIndex  map[int64]xlist.List[messagePtr]
	messagesToResendIndex map[int64]xlist.List[messagePtr]

	topicDescriber TopicDescriber

	wakeupChan         empty.Chan
	ackReceivedChan    empty.Chan
	partitionSplitChan empty.Chan
	shutdown           empty.Chan

	receivedAcks       *guardedList[ack]
	splittedPartitions *guardedList[int64]

	partitions map[int64]*PartitionInfo
	initDone   empty.Chan

	currentSeqNo int64

	isAutoPartitioningEnabled bool
	background                *background.Worker
}

func newWorker(
	ctx context.Context,
	stop context.CancelFunc,
	shutdown empty.Chan,
	topicDescriber TopicDescriber,
	background *background.Worker,
	cfg *MultiWriterConfig,
) *worker {
	if cfg.writersFactory == nil {
		cfg.writersFactory = newBaseWritersFactory()
	}

	if cfg.WriterIdleTimeout == 0 {
		cfg.WriterIdleTimeout = defaultWriterIdleTimeout
	}

	if cfg.MaxQueueLen == 0 {
		cfg.MaxQueueLen = defaultInFlightMessagesBufferSize
	}

	w := &worker{
		writers:               make(map[int64]*writerWrapper),
		inFlightMessages:      xlist.New[message](),
		cfg:                   cfg,
		wakeupChan:            make(empty.Chan, 1),
		ackReceivedChan:       make(empty.Chan, 1),
		partitionSplitChan:    make(empty.Chan, 1),
		shutdown:              shutdown,
		topicDescriber:        topicDescriber,
		ctx:                   ctx,
		stop:                  stop,
		partitions:            make(map[int64]*PartitionInfo),
		initDone:              make(empty.Chan),
		messagesSema:          make(empty.Chan, int64(cfg.MaxQueueLen)/2),
		inFlightMessagesIndex: make(map[int64]xlist.List[messagePtr]),
		messagesToResendIndex: make(map[int64]xlist.List[messagePtr]),
		pendingMessagesIndex:  make(map[int64]xlist.List[messagePtr]),
		background:            background,
		receivedAcks:          newGuardedList[ack](),
		splittedPartitions:    newGuardedList[int64](),
	}

	if cfg.PartitioningKeyHasher == nil {
		cfg.PartitioningKeyHasher = w.getDefaultKeyHasher()
	}

	w.idleWritersSupervisor = newIdleWritersSupervisor(ctx, w, cfg.WriterIdleTimeout)
	background.Start("idle writers supervisor", func(ctx context.Context) {
		w.idleWritersSupervisor.run()
	})

	background.Start("ack receiver", func(ctx context.Context) {
		w.runAckReceiver()
	})
	background.Start("partition splitter", func(ctx context.Context) {
		w.runPartitionSplitter()
	})

	return w
}

func isOperationErrorOverloaded(err error) bool {
	return xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED)
}

func (w *worker) wakeUpAckReceiver() {
	select {
	case w.ackReceivedChan <- empty.Struct{}:
	default:
	}
}

func (w *worker) runAckReceiver() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.ackReceivedChan:
		}

		receivedAcks := w.receivedAcks.Consume()
		w.mu.WithLock(func() {
			for _, ack := range receivedAcks {
				w.onAckReceived(ack.partitionID, ack.seqNo)
			}
		})
	}
}

func (w *worker) wakeUpPartitionSplitter() {
	select {
	case w.partitionSplitChan <- empty.Struct{}:
	default:
	}
}

func (w *worker) runPartitionSplitter() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.partitionSplitChan:
		}

		splittedPartitions := w.splittedPartitions.Consume()
		for _, partitionID := range splittedPartitions {
			err := w.onPartitionSplit(partitionID)
			if err != nil {
				w.mu.WithLock(func() {
					w.err = err
					w.stop()
				})

				return
			}
		}
	}
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

	partitions := make([]int64, 0, len(describeResult.Partitions))

	for _, partition := range describeResult.Partitions {
		var parentID *int64
		if len(partition.ParentPartitionIDs) > 0 {
			parentID = &partition.ParentPartitionIDs[0]
		}

		partitions = append(partitions, partition.PartitionID)
		w.partitions[partition.PartitionID] = &PartitionInfo{
			ID:        partition.PartitionID,
			ParentID:  parentID,
			Children:  partition.ChildPartitionIDs,
			FromBound: partition.FromBound,
			ToBound:   partition.ToBound,
		}
	}

	if err := w.initSeqNo(); err != nil {
		return err
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

func (w *worker) choosePartition(msg message) (partitionID int64, err error) {
	switch {
	case msg.PartitionID != 0:
		return msg.PartitionID, nil
	case msg.Key != "":
		if w.partitionChooser == nil {
			if w.err == nil {
				w.err = fmt.Errorf("partition chooser is not set")
				w.stop()
			}

			return 0, w.err
		}

		partitionID, err = w.partitionChooser.ChoosePartition(msg.Key)
		if err != nil {
			return 0, err
		}
	case w.cfg.CustomChoosePartitionFunc != nil:
		partitionID, err = w.cfg.CustomChoosePartitionFunc(msg.PublicMessage)
		if err != nil {
			return 0, err
		}
	default:
		if w.partitionChooser == nil {
			if w.err == nil {
				w.err = fmt.Errorf("partition chooser is not set")
				w.stop()
			}

			return 0, w.err
		}

		partitionID, err = w.partitionChooser.ChoosePartition(w.cfg.ProducerIDPrefix)
		if err != nil {
			return 0, err
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

func (w *worker) acquireMessage(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		return w.getResultErr()
	case w.messagesSema <- struct{}{}:
		return nil
	}
}

func (w *worker) pushMessage(ctx context.Context, msg message) (err error) {
	var autoSetSeqNo bool
	w.mu.WithLock(func() {
		autoSetSeqNo = w.cfg.AutoSetSeqNo
	})

	switch {
	case !autoSetSeqNo && msg.SeqNo == 0:
		return ErrNoSeqNo
	case autoSetSeqNo && msg.SeqNo != 0:
		return topicwriterinternal.ErrNonZeroSeqNo
	case autoSetSeqNo && msg.SeqNo == 0:
		w.currentSeqNo++
		msg.SeqNo = w.currentSeqNo
	}

	if err := w.acquireMessage(ctx); err != nil {
		return err
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

func (w *worker) notifyOnPartitionSplit(partitionID int64) {
	w.splittedPartitions.PushBack(partitionID)
	w.wakeUpPartitionSplitter()
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
			topicwriterinternal.WithAutoSetSeqNo(false),
			topicwriterinternal.WithOnAckReceivedCallback(func(seqNo int64) {
				w.receivedAcks.PushBack(ack{
					partitionID: partitionID,
					seqNo:       seqNo,
				})

				w.wakeUpAckReceiver()
			}),
			withCustomCheckRetryErrorFunction(func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
				if isOperationErrorOverloaded(args.Error) {
					w.notifyOnPartitionSplit(partitionID)

					return topic.PublicRetryDecisionStop
				}

				var checkErrorResult topic.PublicCheckRetryResult
				w.mu.WithLock(func() {
					if w.cfg.RetrySettings.CheckError != nil {
						checkErrorResult = w.cfg.RetrySettings.CheckError(args)
					}
				})

				return checkErrorResult
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
// must be guarded by w.mu
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

// no concurrent safe
// must be guarded by w.mu
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
		w.releaseMessage()
	}
}

func (w *worker) releaseMessage() {
	<-w.messagesSema
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

func (w *worker) rechoosePartition(msg *message) (err error) {
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

		bufferedMessagesMap = make(map[int64]topicwriterinternal.PublicMessage)
	)

	writer, err := w.getWriter(partitionID)
	if err != nil {
		return err
	}

	bufferedMessages := writer.GetBufferedMessages()
	for _, msg := range bufferedMessages {
		bufferedMessagesMap[msg.PartitionID] = msg
	}

	for inFlightIndexChain.Len() > 0 {
		iter := inFlightIndexChain.Front()

		msg := iter.Value.Value
		if msg.SeqNo < maxSeqNo {
			if msg.ackReceived {
				inFlightIndexChain.Remove(iter)

				continue
			}

			w.onAckReceived(partitionID, msg.SeqNo)

			continue
		}

		if err := w.rechoosePartition(&msg); err != nil {
			return err
		}

		bufferedMessage, ok := bufferedMessagesMap[msg.PartitionID]
		if ok {
			iter.Value.Value.Data = bufferedMessage.Data
		}

		iter.Value.Value.PartitionID = msg.PartitionID
		inFlightMessagesToAdd = append(inFlightMessagesToAdd, iter.Value)
		if iter.Value.Value.sent {
			iter.Value.Value.sent = false
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
	w.releaseInFlightMessages()

	if len(w.pendingMessagesIndex) > 0 || len(w.messagesToResendIndex) > 0 {
		w.wakeup()
	}

	return nil
}

func (w *worker) initSeqNo() error {
	partitions := make([]int64, 0, len(w.partitions))
	for partitionID := range w.partitions {
		partitions = append(partitions, partitionID)
	}

	maxSeqNo, err := w.getMaxSeqNo(partitions)
	if err != nil {
		return err
	}

	for _, partitionID := range partitions {
		w.idleWritersSupervisor.add(partitionID)
	}

	w.currentSeqNo = maxSeqNo

	return nil
}

func (w *worker) getMaxSeqNo(partitions []int64) (maxSeqNo int64, err error) {
	var (
		mu       xsync.Mutex
		errGroup errgroup.Group
	)

	errGroup.SetLimit(10)

	for _, partition := range partitions {
		errGroup.Go(func() error {
			partitionInfo := w.partitions[partition]

			var writer *writerWrapper
			if partitionInfo.Splitted() {
				writer, err = w.createWriterToSplittedPartition(partition)
				if err != nil {
					return err
				}
			} else {
				writer, err = w.getWriter(partition)
				if err != nil {
					return err
				}
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

func (w *worker) createWriterToSplittedPartition(partitionID int64) (*writerWrapper, error) {
	writerCfg := w.cfg.WriterReconnectorConfig
	topicwriterinternal.WithProducerID(w.getProducerID(partitionID))(&writerCfg)

	writer, err := w.cfg.writersFactory.Create(writerCfg)

	return &writerWrapper{
		writer: writer,
	}, err
}

func (w *worker) onPartitionSplit(partitionID int64) (resultErr error) {
	const (
		maxRetries = 5
		retryDelay = 100 * time.Millisecond
	)

	var (
		err            error
		describeResult topictypes.TopicDescription
	)

	for i := range maxRetries {
		describeResult, err = w.topicDescriber(w.ctx, w.cfg.Topic())
		if err == nil {
			break
		}

		if err != nil && i == maxRetries-1 {
			return err
		}
		time.Sleep(retryDelay)
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
			_, err := partitionWriter.WaitInit(w.ctx)
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

func (w *worker) stopWithError(err error) {
	w.mu.WithLock(func() {
		if w.ctx.Err() != nil {
			return
		}

		w.err = err
		w.stop()
	})
}

func (w *worker) flush(ctx context.Context) error {
	waitCh := make(empty.Chan)

	w.mu.WithLock(func() {
		if w.inFlightMessages.Len() == 0 {
			close(waitCh)

			return
		}

		lastInFlightMessage := w.inFlightMessages.Back()
		prevAckCallback := lastInFlightMessage.Value.onAckCallback
		lastInFlightMessage.Value.onAckCallback = func() {
			if prevAckCallback != nil {
				prevAckCallback()
			}

			close(waitCh)
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

func (w *worker) iterateThroughMessagesIndex(
	index map[int64]xlist.List[messagePtr],
	stopFunc func(msg messagePtr) bool,
) error {
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
				if isOperationErrorOverloaded(err) {
					w.notifyOnPartitionSplit(msg.PartitionID)

					break
				}

				return fmt.Errorf("failed to write message: %w", err)
			}

			iter.Value.Value.sent = true
		}

		iter := list.Front()
		for list.Len() > 0 && iter != nil {
			next := iter.Next()
			if iter.Value.Value.sent {
				list.Remove(iter)
			}
			iter = next
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

func (w *worker) step() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.iterateThroughMessagesIndex(
		w.messagesToResendIndex,
		func(msg messagePtr) bool { return false },
	); err != nil {
		return err
	}

	return w.iterateThroughMessagesIndex(
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

func (w *worker) getWritersCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.writers)
}
