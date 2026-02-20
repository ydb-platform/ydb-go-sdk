package topicproducer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

type Message struct {
	topicwriterinternal.PublicMessage
	Key           string
	PartitionID   int64
	OnAckCallback func()
}

type Producer struct {
	ctx              context.Context
	cfg              *ProducerConfig
	closed           atomic.Bool
	partitionChooser PartitionChooser
	worker           *worker
	background       *background.Worker
	shutdown         empty.Chan
}

func NewProducer(cfg *ProducerConfig, topicClient topic.Client) *Producer {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		shutdown    = make(empty.Chan)
		background  = background.NewWorker(ctx, "topic producer background")
	)

	if cfg.SubSessionIdleTimeout == 0 {
		cfg.SubSessionIdleTimeout = defaultSubWriterIdleTimeout
	}

	p := &Producer{
		cfg:        cfg,
		worker:     newWorker(ctx, cancel, shutdown, cfg, topicClient, background),
		shutdown:   shutdown,
		background: background,
	}

	p.background.Start("main worker", func(ctx context.Context) {
		p.worker.run()
	})

	return p
}

type idleWritersSupervisor struct {
	ctx              context.Context
	idleWriters      xlist.List[idleWriterInfo]
	idleWritersIndex map[int64]*xlist.Element[idleWriterInfo]
	mu               xsync.Mutex
	timeout          time.Duration
	background       *background.Worker
	updateChan       empty.Chan
	worker           *worker
}

func newIdleWritersSupervisor(
	ctx context.Context,
	worker *worker,
	idleTimeout time.Duration,
) *idleWritersSupervisor {
	return &idleWritersSupervisor{
		ctx:              ctx,
		idleWriters:      xlist.New[idleWriterInfo](),
		idleWritersIndex: make(map[int64]*xlist.Element[idleWriterInfo]),
		updateChan:       make(empty.Chan, 1),
		worker:           worker,
		timeout:          idleTimeout,
	}
}

func (s *idleWritersSupervisor) add(partitionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wasEmpty := s.idleWriters.Len() == 0
	element := s.idleWriters.PushBack(idleWriterInfo{
		partitionID: partitionID,
		deadline:    time.Now().Add(s.timeout),
	})
	s.idleWritersIndex[partitionID] = element

	if wasEmpty {
		s.updateChan <- empty.Struct{}
	}
}

func (s *idleWritersSupervisor) remove(partitionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, ok := s.idleWritersIndex[partitionID]
	if !ok {
		return
	}

	wasHead := element.Prev() == nil
	s.idleWriters.Remove(element)
	delete(s.idleWritersIndex, partitionID)

	if wasHead {
		s.updateChan <- empty.Struct{}
	}
}

func (s *idleWritersSupervisor) run() {
	for {
		nextTimeout := infiniteTimeout
		s.mu.WithLock(func() {
			if s.idleWriters.Len() > 0 {
				nextTimeout = time.Until(s.idleWriters.Front().Value.deadline)
			}
		})

		select {
		case <-s.ctx.Done():
			return
		case <-s.updateChan:
		case <-time.After(nextTimeout):
		}

		var partitionID *int64
		s.mu.WithLock(func() {
			element := s.idleWriters.Front()
			if element == nil {
				return
			}
			if element.Value.deadline.After(time.Now()) {
				partitionID = &element.Value.partitionID
			}
		})

		if partitionID == nil {
			continue
		}

		s.worker.removeSubWriter(*partitionID)
	}
}

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
	}

	w.idleWritersSupervisor = newIdleWritersSupervisor(ctx, w, cfg.SubSessionIdleTimeout)
	background.Start("idle writers supervisor", func(ctx context.Context) {
		w.idleWritersSupervisor.run()
	})

	return w
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

func (w *worker) getSubWriter(partitionID int64) (subWriter, error) {
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
	writer.inFlightCount++

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

	var sentMessages int
	for msg := w.pendingMessages.Front(); msg != nil; msg = msg.Next() {
		writer, err := w.getSubWriter(msg.Value.Value.PartitionID)
		if err != nil {
			return fmt.Errorf("failed to get sub writer: %w", err)
		}

		err = writer.Write(w.ctx, msg.Value.Value.PublicMessage)
		if err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}

		sentMessages++
	}

	for range sentMessages {
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

func (w *Producer) Write(ctx context.Context, messages ...Message) error {
	for i := range messages {
		if messages[i].Key != "" {
			messages[i].PartitionID = w.partitionChooser.ChoosePartition(messages[i].Key)
		}
	}

	for i := range messages {
		w.worker.pushMessage(messages[i])
	}

	return nil
}

func (w *Producer) Close(ctx context.Context) error {
	if w.closed.Swap(true) {
		return ErrAlreadyClosed
	}

	w.worker.stop()
	w.background.Close(ctx, nil)

	select {
	case <-w.shutdown:
		return w.worker.getResultErr()
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

func (w *Producer) Flush(ctx context.Context) error {
	return w.worker.flush(ctx)
}
