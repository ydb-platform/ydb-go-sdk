package topicmultiwriter

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type MultiWriter struct {
	ctx        context.Context //nolint:containedctx
	cfg        *MultiWriterConfig
	closed     atomic.Bool
	worker     *worker
	background *background.Worker
	shutdown   empty.Chan
}

func NewMultiWriter(topicDescriber TopicDescriber, cfg MultiWriterConfig) (*MultiWriter, error) {
	if cfg.ProducerIDPrefix == "" {
		return nil, fmt.Errorf("%w: producer id prefix is required", ErrInvalidConfiguration)
	}

	var (
		ctx, cancel = context.WithCancel(context.Background())
		shutdown    = make(empty.Chan)
		background  = background.NewWorker(ctx, "topic multiwriter background")
	)

	p := &MultiWriter{
		ctx:        ctx,
		cfg:        &cfg,
		worker:     newWorker(ctx, cancel, shutdown, topicDescriber, background, &cfg),
		shutdown:   shutdown,
		background: background,
	}

	p.background.Start("main worker", func(ctx context.Context) {
		err := p.worker.init()
		if err != nil {
			p.worker.stopWithError(err)

			return
		}

		p.worker.run()
	})

	return p, nil
}

func (p *MultiWriter) Write(ctx context.Context, messages []topicwriterinternal.PublicMessage) error {
	for _, msg := range messages {
		if err := p.worker.pushMessage(ctx, message{
			PublicMessage: msg,
		}); err != nil {
			return err
		}
	}

	if p.cfg.WaitServerAck {
		return p.worker.flush(ctx)
	}

	return nil
}

func (p *MultiWriter) Close(ctx context.Context) error {
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
	if err := p.background.Close(ctx, nil); err != nil {
		return err
	}

	return p.worker.getResultErr()
}

func (p *MultiWriter) Flush(ctx context.Context) error {
	return p.worker.flush(ctx)
}

func (p *MultiWriter) WaitInit(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	currentSeqNo, err := p.worker.waitInitDone(ctx)
	if err != nil {
		return topicwriterinternal.InitialInfo{}, err
	}

	return topicwriterinternal.InitialInfo{
		LastSeqNum: currentSeqNo,
	}, nil
}

func (p *MultiWriter) getWritersCount() int {
	return p.worker.getWritersCount()
}
