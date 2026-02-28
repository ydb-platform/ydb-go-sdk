package topicproducer

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	topicclient "github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

type Message struct {
	topicwriterinternal.PublicMessage
	Key           string
	PartitionID   int64
	OnAckCallback func()
	AckReceived   bool
	Sent          bool
}

type Producer struct {
	ctx        context.Context
	cfg        *ProducerConfig
	closed     atomic.Bool
	worker     *worker
	background *background.Worker
	shutdown   empty.Chan
}

func NewProducer(topicClient topicclient.Client, cfg ProducerConfig) *Producer {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		shutdown    = make(empty.Chan)
		background  = background.NewWorker(ctx, "topic producer background")
	)

	p := &Producer{
		ctx:        ctx,
		cfg:        &cfg,
		worker:     newWorker(ctx, cancel, shutdown, topicClient, background, &cfg),
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
	for _, message := range messages {
		if err := p.worker.pushMessage(ctx, message); err != nil {
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
