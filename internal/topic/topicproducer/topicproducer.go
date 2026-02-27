package topicproducer

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

type Message struct {
	topicwriterinternal.PublicMessage
	Key           string
	PartitionID   int64
	OnAckCallback func()
}

type Producer struct {
	ctx        context.Context
	cfg        *ProducerConfig
	closed     atomic.Bool
	worker     *worker
	background *background.Worker
	shutdown   empty.Chan
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
		err := p.worker.init()
		if err != nil {
			p.worker.err = err
			p.worker.stop()
			return
		}

		p.worker.run()
	})

	return p
}

func (w *Producer) Write(ctx context.Context, messages ...Message) (err error) {
	for _, message := range messages {
		w.worker.pushMessage(message)
	}

	return
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

func (w *Producer) WaitInit(ctx context.Context) error {
	return w.worker.waitInitDone(ctx)
}
