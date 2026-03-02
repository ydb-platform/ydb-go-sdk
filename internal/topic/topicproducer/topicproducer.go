package topicproducer

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type Message struct {
	topicwriterinternal.PublicMessage

	Key         string
	PartitionID int64

	onAckCallback func()
	ackReceived   bool
	sent          bool
}

type Producer struct {
	ctx        context.Context //nolint:containedctx
	cfg        *ProducerConfig
	closed     atomic.Bool
	worker     *worker
	background *background.Worker
	shutdown   empty.Chan
}

func NewProducer(topicDescriber TopicDescriber, cfg ProducerConfig) *Producer {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		shutdown    = make(empty.Chan)
		background  = background.NewWorker(ctx, "topic producer background")
	)

	p := &Producer{
		ctx:        ctx,
		cfg:        &cfg,
		worker:     newWorker(ctx, cancel, shutdown, topicDescriber, background, &cfg),
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

func (p *Producer) Write(ctx context.Context, messages ...Message) error {
	for _, msg := range messages {
		if err := p.worker.pushMessage(ctx, msg); err != nil {
			return err
		}
	}

	if p.cfg.WaitServerAck {
		return p.worker.flush(ctx)
	}

	return nil
}

func (p *Producer) Close(ctx context.Context) error {
	if p.closed.Swap(true) {
		return ErrAlreadyClosed
	}

	defer func() {
		_ = p.worker.closeWriters(ctx)
	}()

	if err := p.worker.flush(ctx); err != nil {
		return err
	}

	p.worker.stop()
	p.background.Close(ctx, nil)

	select {
	case <-p.shutdown:
		return p.worker.getResultErr()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Producer) Flush(ctx context.Context) error {
	return p.worker.flush(ctx)
}

func (p *Producer) WaitInit(ctx context.Context) error {
	return p.worker.waitInitDone(ctx)
}

func (p *Producer) getWritersCount() int {
	return len(p.worker.writers)
}
