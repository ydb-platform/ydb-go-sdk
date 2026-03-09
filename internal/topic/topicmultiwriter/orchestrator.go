package topicmultiwriter

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xhash"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type orchestrator struct {
	ctx  context.Context //nolint:containedctx
	err  error
	stop context.CancelFunc

	cfg *MultiWriterConfig
	mu  *xsync.Mutex

	partitionChooser PartitionChooser
	topicDescriber   TopicDescriber

	partitions map[int64]*PartitionInfo
	initDone   empty.Chan

	currentSeqNo int64

	background *background.Worker

	buf                    *inflightBuffer
	writerPool             *partitionWriterPool
	ackReceiver            *ackReceiver
	partitionSplitReceiver *partitionSplitReceiver
	sender                 *sender
}

//nolint:funlen
func newOrchestrator(
	ctx context.Context,
	stop context.CancelFunc,
	topicDescriber TopicDescriber,
	background *background.Worker,
	cfg *MultiWriterConfig,
) *orchestrator {
	if cfg.writersFactory == nil {
		cfg.writersFactory = newBaseWritersFactory()
	}

	if cfg.WriterIdleTimeout == 0 {
		cfg.WriterIdleTimeout = defaultWriterIdleTimeout
	}

	if cfg.MaxQueueLen == 0 {
		cfg.MaxQueueLen = defaultInFlightMessagesBufferSize
	}

	o := &orchestrator{
		cfg:            cfg,
		mu:             &xsync.Mutex{},
		topicDescriber: topicDescriber,
		ctx:            ctx,
		stop:           stop,
		partitions:     make(map[int64]*PartitionInfo),
		initDone:       make(empty.Chan),
		background:     background,
	}

	o.buf = newInflightBuffer(ctx, o.mu, cfg, func() error { return o.getResultErr() })
	o.ackReceiver = newAckReceiver(ctx, func(partitionID, seqNo int64) {
		o.mu.WithLock(func() {
			o.onAckReceivedNeedLock(partitionID, seqNo)
		})
	})
	o.partitionSplitReceiver = newPartitionSplitReceiver(
		ctx,
		func(partitionID int64) error {
			return o.onPartitionSplit(partitionID)
		},
		o.stopWithError,
	)
	o.writerPool = newPartitionWriterPool(
		ctx,
		cfg,
		background,
		o.ackReceiver.push,
		o.partitionSplitReceiver.push,
		func() {
			o.sender.wakeup()
		},
		o.stopWithError,
	)
	o.sender = newSender(
		ctx,
		o.partitions,
		o.mu,
		o.buf,
		o.writerPool,
		o.partitionSplitReceiver,
		o.stopWithError,
	)

	if cfg.PartitioningKeyHasher == nil {
		cfg.PartitioningKeyHasher = o.getDefaultKeyHasher()
	}

	background.Start("ack receiver", func(ctx context.Context) {
		o.ackReceiver.run()
	})
	background.Start("partition splitter", func(ctx context.Context) {
		o.partitionSplitReceiver.run()
	})
	background.Start("sender", func(ctx context.Context) {
		o.sender.run()
	})

	return o
}

func isOperationErrorOverloaded(err error) bool {
	return xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED)
}

func (o *orchestrator) getDefaultKeyHasher() KeyHasher {
	return func(key string) string {
		// Same as C++ TProducerSettings::DefaultPartitioningKeyHasher:
		// MurmurHash64 with seed 0, result as 8 bytes in big-endian (network byte order)
		lo := xhash.Murmur2Hash64A([]byte(key), 0)
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, lo)

		return string(out)
	}
}

func (o *orchestrator) init() (err error) {
	defer close(o.initDone)

	describeResult, err := o.topicDescriber(o.ctx, o.cfg.Topic())
	if err != nil {
		return err
	}

	for _, partition := range describeResult.Partitions {
		var parentID *int64
		if len(partition.ParentPartitionIDs) > 0 {
			parentID = &partition.ParentPartitionIDs[0]
		}

		o.partitions[partition.PartitionID] = &PartitionInfo{
			ID:        partition.PartitionID,
			ParentID:  parentID,
			Children:  partition.ChildPartitionIDs,
			FromBound: partition.FromBound,
			ToBound:   partition.ToBound,
		}
	}

	if err := o.initSeqNo(); err != nil {
		return err
	}

	isAutoPartitioningEnabled := describeResult.PartitionSettings.AutoPartitioningSettings.AutoPartitioningStrategy !=
		topictypes.AutoPartitioningStrategyDisabled

	switch o.cfg.PartitionChooserStrategy {
	case PartitionChooserStrategyBound:
		o.partitionChooser, err = newBoundPartitionChooser(o.cfg, o.partitions)
		if err != nil {
			return err
		}
	case PartitionChooserStrategyHash:
		if isAutoPartitioningEnabled {
			return ErrHashPartitionChooserNotSupported
		}

		partitionIDs := make([]int64, 0, len(o.partitions))
		for id := range o.partitions {
			partitionIDs = append(partitionIDs, id)
		}

		o.partitionChooser = newHashPartitionChooser(o.cfg, partitionIDs)
	case PartitionChooserStrategyCustom:
		if o.cfg.CustomPartitionChooser == nil {
			return fmt.Errorf("%w: custom partition chooser is not set", ErrInvalidConfiguration)
		}

		o.partitionChooser = o.cfg.CustomPartitionChooser
	}

	return nil
}

func (o *orchestrator) choosePartition(msg message) (partitionID int64, err error) {
	if msg.PartitionID != 0 {
		return msg.PartitionID, nil
	}

	if msg.Key == "" {
		msg.Key = o.cfg.ProducerIDPrefix
	}

	partitionID, err = o.partitionChooser.ChoosePartition(msg)
	if err != nil {
		return 0, fmt.Errorf("choose partition: %w", err)
	}

	return partitionID, nil
}

func (o *orchestrator) pushMessage(ctx context.Context, msg message) (err error) {
	var autoSetSeqNo bool
	o.mu.WithLock(func() {
		autoSetSeqNo = o.cfg.AutoSetSeqNo
	})

	switch {
	case !autoSetSeqNo && msg.SeqNo == 0:
		return ErrNoSeqNo
	case autoSetSeqNo && msg.SeqNo != 0:
		return topicwriterinternal.ErrNonZeroSeqNo
	case autoSetSeqNo && msg.SeqNo == 0:
		o.mu.WithLock(func() {
			o.currentSeqNo++
			msg.SeqNo = o.currentSeqNo
		})
	}

	if err := o.buf.acquireMessage(ctx); err != nil {
		return err
	}

	o.mu.WithLock(func() {
		msg.PartitionID, err = o.choosePartition(msg)
		if err != nil {
			return
		}

		o.buf.pushNeedLock(msg)
		o.sender.wakeup()
	})

	return nil
}

func (o *orchestrator) onAckReceivedNeedLock(partitionID, seqNo int64) {
	indexChain, ok := o.buf.inFlightMessagesIndex[partitionID]
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
		delete(o.buf.inFlightMessagesIndex, partitionID)
		o.writerPool.evict(partitionID)
	}

	o.buf.sweep()
	if len(o.buf.pendingMessagesIndex) > 0 {
		o.sender.wakeup()
	}
}

func (o *orchestrator) getSplittedPartitionAncestors(
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

func (o *orchestrator) addNewPartitions(describeResult *topictypes.TopicDescription, splittedPartitionID int64) {
	for _, partition := range describeResult.Partitions {
		if len(partition.ParentPartitionIDs) > 0 && partition.ParentPartitionIDs[0] == splittedPartitionID {
			o.partitions[partition.PartitionID] = &PartitionInfo{
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

func (o *orchestrator) getSplittedPartitionChildren(
	describeResult *topictypes.TopicDescription,
	partitionID int64,
) []int64 {
	for _, partition := range describeResult.Partitions {
		if partition.PartitionID == partitionID {
			return partition.ChildPartitionIDs
		}
	}

	return nil
}

func (o *orchestrator) rechoosePartition(msg *message) (err error) {
	msg.PartitionID = 0
	msg.PartitionID, err = o.choosePartition(*msg)

	return err
}

func (o *orchestrator) getWriterBufferedMessages(
	partitionID int64,
) (map[int64]topicwriterinternal.PublicMessage, error) {
	writer, err := o.writerPool.get(partitionID, true)
	if err != nil {
		return nil, err
	}

	if writer == nil {
		return nil, nil //nolint:nilnil
	}

	var (
		bufferedMessagesMap = make(map[int64]topicwriterinternal.PublicMessage)
		bufferedMessages    = writer.GetBufferedMessages()
	)

	for _, msg := range bufferedMessages {
		bufferedMessagesMap[msg.PartitionID] = msg
	}

	return bufferedMessagesMap, nil
}

//nolint:funlen
func (o *orchestrator) scheduleResendMessages(partitionID, maxSeqNo int64) (err error) {
	inFlightIndexChain, ok := o.buf.inFlightMessagesIndex[partitionID]
	if !ok {
		return nil
	}

	var (
		inFlightMessagesToAdd []messagePtr
		pendingMessagesToAdd  []messagePtr
		messagesToResendToAdd []messagePtr
	)

	bufferedMessagesMap, err := o.getWriterBufferedMessages(partitionID)
	if err != nil {
		return err
	}

	for inFlightIndexChain.Len() > 0 {
		iter := inFlightIndexChain.Front()

		msg := iter.Value.Value
		if msg.SeqNo < maxSeqNo {
			if msg.ackReceived {
				inFlightIndexChain.Remove(iter)

				continue
			}

			o.onAckReceivedNeedLock(partitionID, msg.SeqNo)

			continue
		}

		if err := o.rechoosePartition(&msg); err != nil {
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
		o.buf.addToInFlightMessagesIndex(inFlightMessagesToAdd[i], inFlightMessagesToAdd[i].Value.PartitionID, true)
	}
	for i := len(pendingMessagesToAdd) - 1; i >= 0; i-- {
		o.buf.addToPendingMessagesIndex(pendingMessagesToAdd[i], pendingMessagesToAdd[i].Value.PartitionID, true)
	}
	for i := len(messagesToResendToAdd) - 1; i >= 0; i-- {
		o.buf.addToMessagesToResendIndex(messagesToResendToAdd[i], messagesToResendToAdd[i].Value.PartitionID, true)
	}

	delete(o.buf.inFlightMessagesIndex, partitionID)
	delete(o.buf.pendingMessagesIndex, partitionID)
	o.buf.sweep()

	if len(o.buf.pendingMessagesIndex) > 0 || len(o.buf.messagesToResendIndex) > 0 {
		o.sender.wakeup()
	}

	return nil
}

func (o *orchestrator) initSeqNo() error {
	partitions := make([]int64, 0, len(o.partitions))
	for partitionID := range o.partitions {
		partitions = append(partitions, partitionID)
	}

	maxSeqNo, err := o.getMaxSeqNo(partitions)
	if err != nil {
		return err
	}

	o.mu.WithLock(func() {
		o.currentSeqNo = maxSeqNo
	})

	for _, partitionID := range partitions {
		o.writerPool.evict(partitionID)
	}

	return nil
}

func (o *orchestrator) getMaxSeqNo(partitions []int64) (maxSeqNo int64, err error) {
	var errGroup errgroup.Group
	errGroup.SetLimit(10)

	for _, partition := range partitions {
		errGroup.Go(func() (resultErr error) {
			partitionInfo := o.partitions[partition]

			var writer *writerWrapper
			if partitionInfo.Splitted() {
				writer, resultErr = o.createWriterToSplittedPartition(partition)
				if resultErr != nil {
					return resultErr
				}
			} else {
				o.mu.WithLock(func() {
					writer, resultErr = o.writerPool.get(partition, false)
				})
			}

			if resultErr != nil {
				return resultErr
			}

			initInfo, err := writer.WaitInit(o.ctx)
			if err != nil {
				return err
			}

			o.mu.WithLock(func() {
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

func (o *orchestrator) createWriterToSplittedPartition(partitionID int64) (*writerWrapper, error) {
	writerCfg := o.cfg.WriterReconnectorConfig
	topicwriterinternal.WithProducerID(o.writerPool.getProducerID(partitionID))(&writerCfg)

	writer, err := o.cfg.writersFactory.Create(writerCfg)

	return &writerWrapper{
		writer: writer,
	}, err
}

func (o *orchestrator) onPartitionSplit(partitionID int64) (resultErr error) {
	const (
		maxRetries = 5
		retryDelay = 100 * time.Millisecond
	)

	var (
		err            error
		describeResult topictypes.TopicDescription
	)

	for i := range maxRetries {
		describeResult, err = o.topicDescriber(o.ctx, o.cfg.Topic())
		if err == nil {
			break
		}

		if err != nil && i == maxRetries-1 {
			return err
		}
		time.Sleep(retryDelay)
	}

	o.mu.WithLock(func() {
		partition := o.partitions[partitionID]
		partition.Locked = true
		partition.Children = o.getSplittedPartitionChildren(&describeResult, partitionID)
		o.addNewPartitions(&describeResult, partitionID)
		for _, child := range partition.Children {
			childPartition := o.partitions[child]
			o.partitionChooser.AddNewPartition(child, childPartition.FromBound, childPartition.ToBound)
		}
		o.partitionChooser.RemovePartition(partitionID)
	})

	ancestors := o.getSplittedPartitionAncestors(&describeResult, partitionID)
	maxSeqNo, err := o.getMaxSeqNo(ancestors)
	if err != nil {
		return err
	}

	o.mu.WithLock(func() {
		partition := o.partitions[partitionID]
		partition.Locked = false
		err = o.scheduleResendMessages(partitionID, maxSeqNo)
		if err != nil {
			resultErr = err

			return
		}

		for _, child := range partition.Children {
			o.partitions[child].Locked = false
		}

		o.writerPool.evict(partitionID)
	})

	return nil
}

func (o *orchestrator) getResultErr() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.err
}

func (o *orchestrator) stopWithError(err error) {
	o.mu.WithLock(func() {
		if o.ctx.Err() != nil {
			return
		}

		o.err = err
		o.stop()
	})
}

func (o *orchestrator) flush(ctx context.Context) error {
	waitCh := make(empty.Chan)

	o.mu.WithLock(func() {
		if o.buf.inFlightMessages.Len() == 0 {
			close(waitCh)

			return
		}

		lastInFlightMessage := o.buf.inFlightMessages.Back()
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
	case <-o.ctx.Done():
		return o.getResultErr()
	case <-waitCh:
		return nil
	}
}

func (o *orchestrator) waitInitDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.initDone:
		return o.getResultErr()
	}
}

// for test purposes
func (o *orchestrator) getWritersCount() int {
	return o.writerPool.getWritersCount()
}
