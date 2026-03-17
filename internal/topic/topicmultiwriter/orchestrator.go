package topicmultiwriter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
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

	multiWriterCfg *MultiWriterConfig
	writerCfg      *topicwriterinternal.WriterReconnectorConfig
	mu             *xsync.Mutex

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
	writerCfg *topicwriterinternal.WriterReconnectorConfig,
	multiWriterCfg *MultiWriterConfig,
) *orchestrator {
	if multiWriterCfg.writersFactory == nil {
		multiWriterCfg.writersFactory = newBaseWritersFactory()
	}

	if multiWriterCfg.WriterIdleTimeout == 0 {
		multiWriterCfg.WriterIdleTimeout = defaultWriterIdleTimeout
	}

	if writerCfg.MaxQueueLen == 0 {
		writerCfg.MaxQueueLen = defaultInFlightMessagesBufferSize
	}

	o := &orchestrator{
		writerCfg:      writerCfg,
		multiWriterCfg: multiWriterCfg,
		mu:             &xsync.Mutex{},
		topicDescriber: topicDescriber,
		ctx:            ctx,
		stop:           stop,
		partitions:     make(map[int64]*PartitionInfo),
		initDone:       make(empty.Chan),
		background:     background,
	}

	o.buf = newInflightBuffer(ctx, o.mu, writerCfg, func() error { return o.getResultErr() })
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
		multiWriterCfg,
		writerCfg,
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

	if multiWriterCfg.PartitioningKeyHasher == nil {
		multiWriterCfg.PartitioningKeyHasher = o.getDefaultKeyHasher()
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

func (o *orchestrator) checkNeedAssignBounds() bool {
	for _, partition := range o.partitions {
		if len(partition.FromBound) == 0 || len(partition.ToBound) == 0 {
			return true
		}
	}

	return false
}

func (o *orchestrator) assignBoundsToPartitions() {
	keyRanges := BuildKeyRangesSplitMerge(len(o.partitions))

	ids := make([]int, 0, len(o.partitions))
	for id := range o.partitions {
		ids = append(ids, int(id))
	}

	sort.Ints(ids)

	idsOrder := make(map[int64]int)
	for i, id := range ids {
		idsOrder[int64(id)] = i
	}

	for id, partition := range o.partitions {
		partition.FromBound = keyRanges[idsOrder[id]].From
		partition.ToBound = keyRanges[idsOrder[id]].To
	}
}

func (o *orchestrator) init() (err error) {
	defer close(o.initDone)

	describeResult, err := o.topicDescriber(o.ctx, o.writerCfg.Topic())
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

	switch o.multiWriterCfg.PartitionChooserStrategy {
	case PartitionChooserStrategyBound:
		if o.checkNeedAssignBounds() {
			o.assignBoundsToPartitions()
		}

		o.partitionChooser = newBoundPartitionChooser(o.multiWriterCfg, o.partitions)
	case PartitionChooserStrategyHash:
		if isAutoPartitioningEnabled {
			return ErrHashPartitionChooserNotSupported
		}

		partitionIDs := make([]int64, 0, len(o.partitions))
		for id := range o.partitions {
			partitionIDs = append(partitionIDs, id)
		}
		o.partitionChooser = newHashPartitionChooser(o.multiWriterCfg, partitionIDs)
	case PartitionChooserStrategyCustom:
		if o.multiWriterCfg.CustomPartitionChooser == nil {
			return fmt.Errorf("%w: custom partition chooser is not set", ErrInvalidConfiguration)
		}

		o.partitionChooser = o.multiWriterCfg.CustomPartitionChooser
	}

	return nil
}

func (o *orchestrator) choosePartition(msg message) (partitionID int64, err error) {
	if o.multiWriterCfg.PartitionChooserStrategy == PartitionChooserStrategyByPartitionID && msg.Key != "" {
		return 0, fmt.Errorf("%w: key is not allowed when writing by partition id is chosen", ErrInvalidConfiguration)
	}

	if o.multiWriterCfg.PartitionChooserStrategy != PartitionChooserStrategyByPartitionID &&
		o.multiWriterCfg.PartitionChooserStrategy != PartitionChooserStrategyCustom &&
		msg.Key == "" {
		return 0, fmt.Errorf("%w: key is required", ErrInvalidConfiguration)
	}

	if msg.PartitionID != 0 ||
		o.multiWriterCfg.PartitionChooserStrategy == PartitionChooserStrategyByPartitionID {
		return msg.PartitionID, nil
	}

	if msg.Key == "" {
		msg.Key = o.multiWriterCfg.ProducerIDPrefix
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
		autoSetSeqNo = o.writerCfg.AutoSetSeqNo
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

	return err
}

func (o *orchestrator) onAckReceivedNeedLock(partitionID, seqNo int64) {
	indexChain, ok := o.buf.inFlightMessagesIndex[partitionID]
	if !ok {
		return
	}

	message := indexChain.Front()
	if message.Value.Value.SeqNo != 0 && message.Value.Value.SeqNo != seqNo {
		panic(fmt.Sprintf("seqNo mismatch, expected: %d, got: %d", message.Value.Value.SeqNo, seqNo))
	}

	message.Value.Value.ackReceived = true

	indexChain.Remove(message)
	if indexChain.Len() == 0 {
		delete(o.buf.inFlightMessagesIndex, partitionID)
		o.writerPool.evict(partitionID)
	}

	partition := o.partitions[partitionID]
	if partition.PendingResend > 0 {
		partition.PendingResend--
		if partition.PendingResend == 0 {
			partition.Locked = false
		}
	}

	o.buf.sweep()
	if len(o.buf.pendingMessagesIndex) > 0 || partition.PendingResend == 0 {
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
	writer, err := o.writerPool.get(partitionID, true, true)
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
		bufferedMessagesMap[msg.SeqNo] = msg
	}

	return bufferedMessagesMap, nil
}

//nolint:funlen
func (o *orchestrator) scheduleResendMessages(
	partitionID,
	maxSeqNo int64,
	bufferedMessagesMap map[int64]topicwriterinternal.PublicMessage,
) (err error) {
	inFlightIndexChain, ok := o.buf.inFlightMessagesIndex[partitionID]
	if !ok {
		return nil
	}

	var (
		inFlightMessagesToAdd    []messagePtr
		messagesToResendToAdd    []messagePtr
		pendingResendByPartition = make(map[int64]int)
	)

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

		bufferedMessage, ok := bufferedMessagesMap[msg.SeqNo]
		if ok {
			iter.Value.Value.Data = bufferedMessage.Data
		}

		iter.Value.Value.PartitionID = msg.PartitionID
		inFlightMessagesToAdd = append(inFlightMessagesToAdd, iter.Value)
		iter.Value.Value.sent = false
		messagesToResendToAdd = append(messagesToResendToAdd, iter.Value)
		pendingResendByPartition[msg.PartitionID]++
		inFlightIndexChain.Remove(iter)
	}

	for i := len(inFlightMessagesToAdd) - 1; i >= 0; i-- {
		o.buf.getInflightMessagesIndex(inFlightMessagesToAdd[i].Value.PartitionID).PushFront(inFlightMessagesToAdd[i])
	}
	for i := len(messagesToResendToAdd) - 1; i >= 0; i-- {
		o.buf.getMessagesToResendIndex(messagesToResendToAdd[i].Value.PartitionID).PushFront(messagesToResendToAdd[i])
	}

	for resendPartitionID, count := range pendingResendByPartition {
		partition := o.partitions[resendPartitionID]
		partition.PendingResend += count
		partition.Locked = true
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
	const (
		maxRetries = 5
		retryDelay = 100 * time.Millisecond
	)

	partitions := make([]int64, 0, len(o.partitions))
	for partitionID := range o.partitions {
		partitions = append(partitions, partitionID)
	}

	var (
		maxSeqNo int64
		err      error
	)

	for i := range maxRetries {
		maxSeqNo, err = o.getMaxSeqNo(partitions)
		if err == nil {
			break
		}

		if !isOperationErrorOverloaded(err) || i == maxRetries-1 {
			return err
		}

		for _, partitionID := range partitions {
			o.writerPool.forceEvict(partitionID)
		}
		time.Sleep(retryDelay)
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

			var seqNoAlreadyCached bool
			o.mu.WithLock(func() {
				seqNoAlreadyCached = partitionInfo.CachedMaxSeqNo != 0
				maxSeqNo = max(maxSeqNo, partitionInfo.CachedMaxSeqNo)
			})
			if seqNoAlreadyCached {
				return nil
			}

			var writer *writerWrapper
			if partitionInfo.Splitted() {
				writer, resultErr = o.writerPool.get(partition, false, false)
				if resultErr != nil {
					return resultErr
				}
			} else {
				o.mu.WithLock(func() {
					writer, resultErr = o.writerPool.get(partition, true, false)
				})
			}

			if resultErr != nil {
				return resultErr
			}

			initInfo, err := writer.WaitInitInfo(o.ctx)
			if err != nil {
				return err
			}

			o.mu.WithLock(func() {
				maxSeqNo = max(maxSeqNo, initInfo.LastSeqNum)
				partitionInfo.CachedMaxSeqNo = initInfo.LastSeqNum
			})

			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return 0, err
	}

	return maxSeqNo, nil
}

func (o *orchestrator) describeTopicWithRetries(splitPartitionID int64) (topictypes.TopicDescription, error) {
	const (
		maxRetries = 5
		retryDelay = 100 * time.Millisecond
	)

	for range maxRetries {
		describeResult, err := o.topicDescriber(o.ctx, o.writerCfg.Topic())
		if err == nil {
			var needRetry bool
			for _, partition := range describeResult.Partitions {
				if partition.PartitionID == splitPartitionID {
					needRetry = len(partition.ChildPartitionIDs) == 0

					break
				}
			}

			if !needRetry {
				return describeResult, nil
			}
		}

		time.Sleep(retryDelay)
	}

	return topictypes.TopicDescription{}, errors.New("failed to describe topic")
}

func (o *orchestrator) onPartitionSplit(partitionID int64) (resultErr error) {
	var (
		isAlreadySplitted   bool
		bufferedMessagesMap map[int64]topicwriterinternal.PublicMessage
	)

	describeResult, err := o.describeTopicWithRetries(partitionID)
	if err != nil {
		return err
	}

	o.mu.WithLock(func() {
		partition := o.partitions[partitionID]
		if partition.Splitted() {
			isAlreadySplitted = true

			return
		}

		partition.Locked = true
		partition.Children = o.getSplittedPartitionChildren(&describeResult, partitionID)
		o.addNewPartitions(&describeResult, partitionID)
		for _, child := range partition.Children {
			childPartition := o.partitions[child]
			o.partitionChooser.AddNewPartition(child, childPartition.FromBound, childPartition.ToBound)
		}
		o.partitionChooser.RemovePartition(partitionID)
		bufferedMessagesMap, err = o.getWriterBufferedMessages(partitionID)
	})

	if err != nil || isAlreadySplitted {
		return err
	}

	ancestors := o.getSplittedPartitionAncestors(&describeResult, partitionID)
	maxSeqNo, err := o.getMaxSeqNo(ancestors)
	if err != nil {
		return err
	}

	o.mu.WithLock(func() {
		partition := o.partitions[partitionID]
		err = o.scheduleResendMessages(partitionID, maxSeqNo, bufferedMessagesMap)
		if err != nil {
			resultErr = err

			return
		}

		partition.Locked = false
		for _, child := range partition.Children {
			o.partitions[child].Locked = false
		}

		for _, ancestor := range ancestors {
			o.writerPool.evict(ancestor)
		}
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
