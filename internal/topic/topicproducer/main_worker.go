package topicproducer

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
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

	inFlightMessages      xlist.List[Message]
	inFlightMessagesIndex map[int64]xlist.List[messagePtr]
	pendingMessages       xlist.List[messagePtr]

	topicClient topic.Client
	topicPath   string

	msgChan  empty.Chan
	shutdown empty.Chan

	currentSeqNo int64
	partitions   map[int64]PartitionInfo
	initDone     atomic.Bool

	isAutoPartitioningEnabled bool
}

func newWorker(
	ctx context.Context,
	stop context.CancelFunc,
	shutdown empty.Chan,
	cfg *ProducerConfig,
	topicClient topic.Client,
	background *background.Worker,
) *worker {
	w := &worker{
		subWriters:       make(map[int64]*subWriterWrapper),
		inFlightMessages: xlist.New[Message](),
		cfg:              cfg,
		msgChan:          make(empty.Chan, 1),
		shutdown:         shutdown,
		topicClient:      topicClient,
		ctx:              ctx,
		stop:             stop,
		partitions:       make(map[int64]PartitionInfo),
	}

	w.idleWritersSupervisor = newIdleWritersSupervisor(ctx, w, cfg.SubSessionIdleTimeout)
	background.Start("idle writers supervisor", func(ctx context.Context) {
		w.idleWritersSupervisor.run()
	})

	return w
}

func (w *worker) init() (err error) {
	defer func() {
		if err == nil {
			w.initDone.Store(true)
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

		w.partitions[partition.PartitionID] = PartitionInfo{
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

	return
}

func (w *worker) pushMessage(msg Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	newElement := w.inFlightMessages.PushBack(msg)
	list, ok := w.inFlightMessagesIndex[msg.PartitionID]
	if !ok {
		newList := xlist.New[messagePtr]()
		newList.PushBack(newElement)
		w.inFlightMessagesIndex[msg.PartitionID] = newList
	} else {
		list.PushBack(newElement)
	}
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
	writer, err := w.topicClient.StartWriter(
		w.cfg.TopicPath,
		topicoptions.WithWriterPartitionID(partitionID),
		topicoptions.WithWriterProducerID(w.getProducerID(partitionID)),
		topicoptions.WithOnAckReceivedCallback(func(seqNo int64) {
			w.onAckReceived(partitionID, seqNo)
		}),
		topicoptions.WithWriterCheckRetryErrorFunction(func(args topicoptions.CheckErrorRetryArgs) topicoptions.CheckErrorRetryResult {
			if ydb.IsOperationErrorOverloaded(args.Error) {
				w.onPartitionSplit(partitionID)
				return topicoptions.CheckErrorRetryDecisionStop
			}
			return topicoptions.CheckErrorRetryDecisionDefault
		}),
	)
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

	w.inFlightMessages.Remove(message.Value)
	indexChain.Remove(indexChain.Front())
	if indexChain.Len() == 0 {
		delete(w.inFlightMessagesIndex, partitionID)
	}

	if message.Value.Value.OnAckCallback != nil {
		message.Value.Value.OnAckCallback()
	}
}

func (w *worker) onPartitionSplit(partitionID int64) {
}

func (w *worker) getSubWriter(partitionID int64) (*subWriterWrapper, error) {
	writer, ok := w.subWriters[partitionID]
	if !ok {
		writer, err := w.createSubWriter(partitionID)
		if err != nil {
			return nil, err
		}
		w.subWriters[partitionID] = &subWriterWrapper{
			subWriter: writer,
		}
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
		lastInFlightMessage.Value.OnAckCallback = func() {
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

	for w.pendingMessages.Len() > 0 {
		msg := w.pendingMessages.Front()

		writer, err := w.getSubWriter(msg.Value.Value.PartitionID)
		if err != nil {
			return fmt.Errorf("failed to get sub writer: %w", err)
		}

		err = writer.Write(w.ctx, msg.Value.Value.PublicMessage)
		if err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}

		writer.inFlightCount++
		w.pendingMessages.Remove(w.pendingMessages.Front())
	}

	return nil
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
