package topicmultiwriter

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type orchestrator struct {
	ctx  context.Context //nolint:containedctx
	err  error
	stop context.CancelFunc

	multiWriterCfg *MultiWriterConfig
	writerCfg      *topicwriterinternal.WriterReconnectorConfig
	mu             *xsync.Mutex

	// Source.NewRouter and topology updates run outside mu.
	// Source and Router synchronize internally; mu protects only writer state.
	// Source metadata reads and NotifySessionError never perform I/O or callbacks under mu.
	source *partition.Source
	router *partition.Router

	writeStates map[int64]*PartitionInfo
	initDone    empty.Chan

	currentSeqNo int64

	background *background.Worker

	buf         *inflightBuffer
	writerPool  *partitionWriterPool
	ackReceiver *ackReceiver
	sender      *sender
}

func newOrchestrator(
	ctx context.Context,
	stop context.CancelFunc,
	source *partition.Source,
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
		ctx:            ctx,
		stop:           stop,
		writeStates:    make(map[int64]*PartitionInfo),
		initDone:       make(empty.Chan),
		background:     background,
		source:         source,
	}

	o.buf = newInflightBuffer(ctx, o.mu, writerCfg, func() error { return o.getResultErr() })
	o.ackReceiver = newAckReceiver(func(partitionID, seqNo int64) {
		o.mu.WithLock(func() {
			o.onAckReceivedNeedLock(partitionID, seqNo)
		})
	})
	o.writerPool = newPartitionWriterPool(
		ctx,
		multiWriterCfg,
		writerCfg,
		background,
		o.ackReceiver.push,
		o.source,
		func() {
			o.sender.wakeup()
		},
		o.stopWithError,
	)
	o.sender = newSender(
		ctx,
		o.writeStates,
		o.mu,
		o.buf,
		o.writerPool,
		o.source,
		o.stopWithError,
	)

	return o
}

func (o *orchestrator) startWorkers() {
	o.background.Start("route change watcher", func(ctx context.Context) {
		o.watchRouteChanges(ctx)
	})
	o.background.Start("ack receiver", func(ctx context.Context) {
		o.ackReceiver.run(o.ctx)
	})
	o.background.Start("sender", func(ctx context.Context) {
		o.sender.run()
	})
}

func (o *orchestrator) init() (err error) {
	defer close(o.initDone)

	router, err := o.source.NewRouter(o.ctx, o.multiWriterCfg.PartitionChooser)
	if err != nil {
		o.stopWithError(err)

		return err
	}

	o.mu.WithLock(func() {
		o.router = router
	})

	if err := o.initSeqNo(); err != nil {
		o.stopWithError(err)

		return err
	}

	o.startWorkers()

	return nil
}

func (o *orchestrator) choosePartition(msg message) (partitionID int64, err error) {
	if msg.Key == "" {
		msg.Key = o.multiWriterCfg.ProducerIDPrefix
	}

	partitionID, err = o.router.ChoosePartition(msg.PublicMessage)
	if err != nil {
		return 0, fmt.Errorf("choose partition: %w", err)
	}

	// The router validates the route; only writer state is created locally.
	o.writeStateNeedLock(partitionID)

	return partitionID, nil
}

func (o *orchestrator) pushMessage(ctx context.Context, msg message) (err error) {
	acquired := false
	defer func() {
		if err != nil && acquired {
			o.buf.releaseMessage()
		}
	}()

	autoSetSeqNo := o.writerCfg.AutoSetSeqNo

	switch {
	case !autoSetSeqNo && msg.SeqNo == 0:
		return ErrNoSeqNo
	case autoSetSeqNo && msg.SeqNo != 0:
		return topicwriterinternal.ErrNonZeroSeqNo
	}

	if err := o.buf.acquireMessage(ctx); err != nil {
		return err
	}
	acquired = true

	if o.writerCfg.AutoSetCreatedTime {
		if err := topicwritercommon.FillCreatedAt(
			[]topicwritercommon.MessageWithDataContent{msg.MessageWithDataContent},
			o.writerCfg.Now(),
			false,
		); err != nil {
			return err
		}
	}

	if msg.Metadata == nil {
		msg.Metadata = make(map[string][]byte)
	}
	o.mu.WithLock(func() {
		msg.PartitionID, err = o.choosePartition(msg)
	})
	if err != nil {
		return err
	}

	// saveMessageContent must run after choosePartition: BoundPartitionChooser may
	// write choose_partition_key into msg.Metadata; CacheMessageData (inside saveMessageContent)
	// freezes Metadata and would discard that key if it ran earlier. Keeping this outside
	// o.mu avoids holding the lock over user io.Reader reads.
	if err := o.saveMessageContent(&msg); err != nil {
		return err
	}
	o.mu.WithLock(func() {
		if autoSetSeqNo {
			o.currentSeqNo++
			msg.SeqNo = o.currentSeqNo
		} else {
			err = o.reserveSeqNoNeedLock(msg.PartitionID, msg.SeqNo)
			if err != nil {
				return
			}
		}
		o.buf.pushNeedLock(msg)
		o.sender.wakeup()
		acquired = false
	})

	return err
}

func (o *orchestrator) saveMessageContent(msg *message) error {
	tracer := o.writerCfg.Tracer
	if tracer == nil {
		tracer = &trace.Topic{}
	}
	logCtx := o.writerCfg.LogContext
	onCompressDone := gtrace.TopicOnWriterCompressMessages(
		tracer,
		&logCtx,
		o.multiWriterCfg.ProducerIDPrefix,
		"",
		rawtopiccommon.CodecRaw.ToInt32(),
		msg.SeqNo,
		1,
		trace.TopicWriterCompressMessagesReasonCompressDataOnWriteReadData,
	)
	// Materialize raw payload before the message is exposed to writer reconnector.
	// This keeps multiwriter-owned messages resendable even if a downstream writer
	// reads the original reader and fails before enqueueing to its own buffer.
	err := msg.CacheMessageData(rawtopiccommon.CodecRaw)
	onCompressDone(err)
	if err != nil {
		return err
	}

	return nil
}

func (o *orchestrator) onAckReceivedNeedLock(partitionID, seqNo int64) {
	indexChain, ok := o.buf.inFlightMessagesIndex[partitionID]
	if !ok {
		return
	}

	message := indexChain.Front()
	if message == nil {
		return
	}

	if message.Value.Value.SeqNo != 0 && message.Value.Value.SeqNo != seqNo {
		panic(fmt.Sprintf("seqNo mismatch, expected: %d, got: %d", message.Value.Value.SeqNo, seqNo))
	}

	message.Value.Value.ackReceived = true

	indexChain.Remove(message)
	if indexChain.Len() == 0 {
		delete(o.buf.inFlightMessagesIndex, partitionID)
		o.writerPool.evict(partitionID)
	}

	partition := o.writeStates[partitionID]
	if partition != nil && partition.PendingResend > 0 {
		partition.PendingResend--
	}

	o.buf.sweep()
	if len(o.buf.pendingMessagesIndex) > 0 || (partition != nil && partition.PendingResend == 0) {
		o.sender.wakeup()
	}
}

// writeStateNeedLock returns writer state without copying topology from Source.
func (o *orchestrator) writeStateNeedLock(partitionID int64) *PartitionInfo {
	state := o.writeStates[partitionID]
	if state == nil {
		state = &PartitionInfo{}
		o.writeStates[partitionID] = state
	}

	return state
}

func (o *orchestrator) rechoosePartition(msg *message) (err error) {
	msg.PartitionID = 0
	msg.PartitionID, err = o.choosePartition(*msg)

	return err
}

func (o *orchestrator) reserveSeqNoNeedLock(partitionID, seqNo int64) error {
	partition := o.writeStates[partitionID]
	if partition == nil {
		return fmt.Errorf("partition not found: %d", partitionID)
	}
	if seqNo <= partition.LastQueuedSeqNo {
		return fmt.Errorf(
			"%w: seqNo %d <= last seqNo %d for partition %d",
			ErrUnorderedSeqNo,
			seqNo,
			partition.LastQueuedSeqNo,
			partitionID,
		)
	}

	partition.LastQueuedSeqNo = seqNo

	return nil
}

//nolint:funlen
func (o *orchestrator) scheduleResendMessages(
	partitionID,
	maxSeqNo int64,
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
		if msg.SeqNo <= maxSeqNo {
			if msg.ackReceived {
				o.buf.sweep()

				continue
			}

			o.onAckReceivedNeedLock(partitionID, msg.SeqNo)

			continue
		}

		if err := o.rechoosePartition(&msg); err != nil {
			return err
		}

		iter.Value.Value.PartitionID = msg.PartitionID
		if partition := o.writeStates[msg.PartitionID]; partition != nil {
			partition.LastQueuedSeqNo = max(partition.LastQueuedSeqNo, msg.SeqNo)
		}
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
		partition := o.writeStates[resendPartitionID]
		partition.PendingResend += count
	}

	delete(o.buf.inFlightMessagesIndex, partitionID)
	delete(o.buf.pendingMessagesIndex, partitionID)
	delete(o.buf.messagesToResendIndex, partitionID)
	o.buf.sweep()

	if len(o.buf.pendingMessagesIndex) > 0 || len(o.buf.messagesToResendIndex) > 0 {
		o.sender.wakeup()
	}

	return nil
}

// seqNoReadError retains the partition that failed so initialization can refresh
// that partition's topology without coupling seqNo sessions to Source.
type seqNoReadError struct {
	partitionID int64
	active      bool
	err         error
}

func (e *seqNoReadError) Error() string {
	return fmt.Sprintf("read seqNo for partition %d: %v", e.partitionID, e.err)
}

func (e *seqNoReadError) Unwrap() error {
	return e.err
}

func (o *orchestrator) initSeqNo() error {
	for {
		partitions, err := o.source.Partitions(o.ctx)
		if err != nil {
			return err
		}

		maxSeqNo, err := o.getMaxSeqNo(partitions.All())
		if err == nil {
			o.mu.WithLock(func() {
				o.currentSeqNo = maxSeqNo
			})

			return nil
		}

		var readErr *seqNoReadError
		if !errors.As(err, &readErr) || !readErr.active {
			return err
		}
		waitForSplit := o.source.NotifySessionError(o.ctx, readErr.partitionID, readErr.err)
		if waitForSplit == nil {
			return err
		}
		if err := waitForSplit(o.ctx); err != nil {
			return err
		}
		// Source has published the children; retry with a fresh partition list.
	}
}

func (o *orchestrator) getMaxSeqNo(partitions []partition.Partition) (maxSeqNo int64, err error) {
	var errGroup errgroup.Group
	errGroup.SetLimit(10)

	for _, topicPartition := range partitions {
		errGroup.Go(func() error {
			partitionID := topicPartition.ID()
			active := topicPartition.IsActive()
			var (
				partitionInfo      *PartitionInfo
				seqNoAlreadyCached bool
			)

			o.mu.WithLock(func() {
				partitionInfo = o.writeStateNeedLock(partitionID)
				seqNoAlreadyCached = partitionInfo.CachedMaxSeqNo != 0
				maxSeqNo = max(maxSeqNo, partitionInfo.CachedMaxSeqNo)
			})
			if seqNoAlreadyCached {
				return nil
			}

			seqNo, err := o.writerPool.readSeqNo(o.ctx, partitionID, active)
			if err != nil {
				return &seqNoReadError{
					partitionID: partitionID,
					active:      active,
					err:         err,
				}
			}

			o.mu.WithLock(func() {
				maxSeqNo = max(maxSeqNo, seqNo)
				partitionInfo.CachedMaxSeqNo = seqNo
			})

			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return 0, err
	}

	return maxSeqNo, nil
}

func (o *orchestrator) watchRouteChanges(ctx context.Context) {
	for {
		partitionID, err := o.router.WaitForRouteChange()
		if err != nil {
			if ctx.Err() == nil {
				o.stopWithError(err)
			}

			return
		}
		if err := o.recoverAfterSplit(partitionID); err != nil {
			o.stopWithError(err)

			return
		}
	}
}

// recoverAfterSplit restores this writer's messages after Source has confirmed the topology.
func (o *orchestrator) recoverAfterSplit(partitionID int64) (resultErr error) {
	partitions, err := o.source.Partitions(o.ctx)
	if err != nil {
		return err
	}
	topicPartition := partitions.ByPartitionID(partitionID)
	partitionsToRecover := append(partition.List{topicPartition}, topicPartition.Parents()...)

	o.mu.WithLock(func() {
		o.writeStateNeedLock(partitionID).Locked = true
	})

	// Close obsolete working sessions before opening seqNo sessions with the same producer IDs.
	// Closing sessions must not hold the writer mutex.
	for _, partitionIDToRecover := range partitionsToRecover.IDs() {
		o.writerPool.forceEvict(partitionIDToRecover)
	}
	maxSeqNo, err := o.getMaxSeqNo(partitionsToRecover)
	if err != nil {
		return err
	}
	o.mu.WithLock(func() {
		if resultErr = o.scheduleResendMessages(partitionID, maxSeqNo); resultErr != nil {
			return
		}

		state := o.writeStates[partitionID]
		state.Locked = false
	})
	if resultErr != nil {
		return resultErr
	}

	o.sender.wakeup()

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
