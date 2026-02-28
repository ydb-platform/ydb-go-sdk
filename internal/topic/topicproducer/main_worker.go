package topicproducer

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/spaolacci/murmur3"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	topicclient "github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"golang.org/x/sync/errgroup"
)

type messagePtr *xlist.Element[Message]

type worker struct {
	ctx  context.Context
	err  error
	stop context.CancelFunc

	subWriters            map[int64]*subWriterWrapper
	idleWritersSupervisor *idleWritersSupervisor
	cfg                   *ProducerConfig
	mu                    xsync.Mutex
	messagesSemaphore     *xsync.SoftWeightedSemaphore
	writerOpts            []topicwriterinternal.PublicWriterOption

	partitionChooser PartitionChooser

	inFlightMessages      xlist.List[Message]
	inFlightMessagesIndex map[int64]xlist.List[messagePtr]
	pendingMessages       xlist.List[messagePtr]

	topicClient topicclient.Client
	topicPath   string

	msgChan  empty.Chan
	shutdown empty.Chan

	partitions map[int64]*PartitionInfo
	initDone   chan struct{}

	isAutoPartitioningEnabled bool
}

func newWorker(
	ctx context.Context,
	stop context.CancelFunc,
	shutdown empty.Chan,
	topicClient topicclient.Client,
	background *background.Worker,
	cfg *ProducerConfig,
) *worker {
	w := &worker{
		subWriters:        make(map[int64]*subWriterWrapper),
		inFlightMessages:  xlist.New[Message](),
		cfg:               cfg,
		msgChan:           make(empty.Chan, 1),
		shutdown:          shutdown,
		topicClient:       topicClient,
		ctx:               ctx,
		stop:              stop,
		partitions:        make(map[int64]*PartitionInfo),
		initDone:          make(chan struct{}),
		messagesSemaphore: xsync.NewSoftWeightedSemaphore(int64(cfg.MaxQueueLen) / 2),
	}

	if cfg.subWritersFactory == nil {
		cfg.subWritersFactory = newBaseSubWritersFactory(topicClient)
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
	defer func() {
		if err == nil {
			close(w.initDone)
		}
	}()

	describeResult, err := w.topicClient.Describe(w.ctx, w.cfg.TopicPath)
	if err != nil {
		return
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

	w.isAutoPartitioningEnabled =
		describeResult.PartitionSettings.AutoPartitioningSettings.AutoPartitioningStrategy !=
			topictypes.AutoPartitioningStrategyDisabled

	switch w.cfg.PartitionChooserStrategy {
	case PartitionChooserStrategyBound:
		w.partitionChooser, err = newBoundPartitionChooser(w.cfg, w.partitions)
		if err != nil {
			w.err = err
			w.stop()
			return
		}
	case PartitionChooserStrategyHash:
		w.partitionChooser = newHashPartitionChooser(w.cfg, uint64(len(w.partitions)))
	}

	return
}

func (w *worker) choosePartition(msg Message) (partitionID int64, err error) {
	switch {
	case msg.PartitionID != 0:
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
func (w *worker) addToInFlightMessagesIndex(newElement messagePtr, toPartition int64) {
	list, ok := w.inFlightMessagesIndex[toPartition]
	if !ok {
		list = xlist.New[messagePtr]()
		w.inFlightMessagesIndex[toPartition] = list
	}
	list.PushBack(newElement)
}

func (w *worker) pushMessage(ctx context.Context, msg Message) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	msg.PartitionID, err = w.choosePartition(msg)
	if err != nil {
		return
	}

	if err := w.messagesSemaphore.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("ydb: can not add message due to queue len overflow: %w", err)
	}

	newElement := w.inFlightMessages.PushBack(msg)
	w.addToInFlightMessagesIndex(newElement, msg.PartitionID)
	w.pendingMessages.PushBack(newElement)

	select {
	case w.msgChan <- empty.Struct{}:
	default:
	}

	return nil
}

func (w *worker) removeSubWriter(partitionID int64) {
	var writerToClose subWriter

	w.mu.WithLock(func() {
		writer, ok := w.subWriters[partitionID]
		if !ok {
			return
		}
		writerToClose = writer
		delete(w.subWriters, partitionID)
	})

	if err := writerToClose.Close(w.ctx); err != nil {
		w.mu.WithLock(func() {
			w.err = err
			w.stop()
		})
		return
	}
}

func (w *worker) getProducerID(partitionID int64) string {
	return fmt.Sprintf("%s_%d", w.cfg.ProducerIDPrefix, partitionID)
}

func (w *worker) createSubWriter(partitionID int64) (subWriter, error) {
	withCustomCheckRetryErrorFunction := func(callback topic.PublicCheckErrorRetryFunction) topicwriterinternal.PublicWriterOption {
		return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
			cfg.RetrySettings.CheckError = callback
		}
	}

	subWriterOpts := w.writerOpts
	subWriterOpts = append(
		subWriterOpts,
		topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithPartitionID(partitionID)),
		topicwriterinternal.WithProducerID(w.getProducerID(partitionID)),
		topicwriterinternal.WithOnAckReceivedCallback(func(seqNo int64) {
			w.cfg.OnAckReceivedCallback(seqNo)
			w.onAckReceived(partitionID, seqNo)
		}),
		withCustomCheckRetryErrorFunction(func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
			if ydb.IsOperationErrorOverloaded(args.Error) {
				w.onPartitionSplit(partitionID)
				return topic.PublicRetryDecisionStop
			}
			return w.cfg.RetrySettings.CheckError(args)
		}),
		topicwriterinternal.WithMaxQueueLen(w.cfg.MaxQueueLen/2),
	)

	writer, err := w.cfg.subWritersFactory.Create(w.topicPath, subWriterOpts...)
	if err != nil {
		return nil, err
	}

	return writer, nil
}

func (w *worker) onAckReceived(partitionID, seqNo int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	indexChain, ok := w.inFlightMessagesIndex[partitionID]
	if !ok {
		return
	}

	message := indexChain.Front()
	if message.Value.Value.SeqNo != 0 && message.Value.Value.SeqNo != seqNo {
		panic("seq no mismatch") // TODO: maybe not panic here?
	}

	writer, ok := w.subWriters[partitionID]
	if ok {
		writer.inFlightCount--
		if writer.inFlightCount == 0 {
			w.idleWritersSupervisor.add(partitionID)
		}
	}

	message.Value.Value.AckReceived = true

	w.inFlightMessages.Remove(message.Value)
	indexChain.Remove(indexChain.Front())
	if indexChain.Len() == 0 {
		delete(w.inFlightMessagesIndex, partitionID)
	}

	w.releaseInFlightMessages()
}

func (w *worker) releaseInFlightMessages() {
	for w.inFlightMessages.Len() > 0 {
		front := w.inFlightMessages.Front()
		if !front.Value.AckReceived {
			return
		}

		if front.Value.OnAckCallback != nil {
			front.Value.OnAckCallback()
		}

		w.inFlightMessages.Remove(front)
		w.messagesSemaphore.Release(1)
	}
}

func (w *worker) getSplittedPartitionAncestors(describeResult *topictypes.TopicDescription, partitionID int64) ([]int64, error) {
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

	return ancestors, nil
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

func (w *worker) scheduleResendMessages(partitionID, maxSeqNo int64) (err error) {
	inFlightIndexChain, ok := w.inFlightMessagesIndex[partitionID]
	if !ok {
		return nil
	}

	var toResend []messagePtr

	for iter := inFlightIndexChain.Front(); iter != nil; iter = iter.Next() {
		msg := iter.Value.Value
		if msg.SeqNo < maxSeqNo {
			continue
		}

		msg.PartitionID = 0
		msg.PartitionID, err = w.choosePartition(msg)
		if err != nil {
			return
		}

		iter.Value.Value.PartitionID = msg.PartitionID
		w.addToInFlightMessagesIndex(iter.Value, msg.PartitionID)
		toResend = append(toResend, iter.Value)
	}

	for i := len(toResend) - 1; i >= 0; i-- {
		w.pendingMessages.PushFront(toResend[i])
	}

	return nil
}

func (w *worker) onPartitionSplit(partitionID int64) (resultErr error) {
	describeResult, err := w.topicClient.Describe(w.ctx, w.cfg.TopicPath)
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

	ancestors, err := w.getSplittedPartitionAncestors(&describeResult, partitionID)
	if err != nil {
		return err
	}

	var (
		maxSeqNo int64
		mu       xsync.Mutex
		errGroup errgroup.Group
	)

	for _, ancestor := range ancestors {
		errGroup.Go(func() error {
			writer, err := w.topicClient.StartWriter(
				w.cfg.TopicPath,
				topicwriterinternal.WithProducerID(w.getProducerID(ancestor)),
			)
			if err != nil {
				return err
			}

			initInfo, err := writer.WaitInitInfo(w.ctx)
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
		return err
	}

	w.mu.WithLock(func() {
		partition := w.partitions[partitionID]
		partition.Locked = false
		w.scheduleResendMessages(partitionID, maxSeqNo)

		for _, child := range partition.Children {
			w.partitions[child].Locked = false
		}

		if w.pendingMessages.Len() > 0 {
			select {
			case w.msgChan <- empty.Struct{}:
			default:
			}
		}
	})

	return nil
}

func (w *worker) getSubWriter(partitionID int64) (*subWriterWrapper, error) {
	writer, ok := w.subWriters[partitionID]
	if !ok {
		writer, err := w.createSubWriter(partitionID)
		if err != nil {
			return nil, err
		}
		wrapper := &subWriterWrapper{
			subWriter: writer,
		}
		w.subWriters[partitionID] = wrapper

		go func() {
			err = writer.WaitInit(w.ctx)
			w.mu.WithLock(func() {
				if err != nil {
					w.err = err
					w.stop()
					return
				}
				wrapper.initDone = true
			})
		}()
	}

	w.idleWritersSupervisor.remove(partitionID)
	return writer, nil
}

func (w *worker) getResultErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.err
}

func (w *worker) flush(ctx context.Context) error {
	waitCh := make(empty.Chan, 1)

	w.mu.WithLock(func() {
		lastInFlightMessage := w.inFlightMessages.Back()
		prevAckCallback := lastInFlightMessage.Value.OnAckCallback
		lastInFlightMessage.Value.OnAckCallback = func() {
			if prevAckCallback != nil {
				prevAckCallback()
			}

			waitCh <- empty.Struct{}
		}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (w *worker) step() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for iter := w.pendingMessages.Front(); iter != nil; iter = iter.Next() {
		msg := iter.Value.Value

		if w.partitions[msg.PartitionID].Locked {
			continue
		}

		writer, err := w.getSubWriter(msg.PartitionID)
		if err != nil {
			return fmt.Errorf("failed to get sub writer: %w", err)
		}

		if !writer.initDone {
			continue
		}

		err = writer.Write(w.ctx, msg.PublicMessage)
		if err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}

		writer.inFlightCount++
		w.pendingMessages.Remove(iter)
	}

	return nil
}

func (w *worker) waitInitDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.initDone:
		return nil
	}
}

func (w *worker) run() {
	defer close(w.shutdown)

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.msgChan:
		}

		if err := w.step(); err != nil {
			w.err = err
			w.stop()
			return
		}
	}
}
