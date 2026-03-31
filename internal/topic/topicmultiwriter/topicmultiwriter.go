package topicmultiwriter

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type MultiWriter struct {
	ctx          context.Context //nolint:containedctx
	cfg          *MultiWriterConfig
	writerCfg    *topicwriterinternal.WriterReconnectorConfig
	encoders     *topicwritercommon.MultiEncoder
	closed       atomic.Bool
	orchestrator *orchestrator
	background   *background.Worker
}

func NewMultiWriter(
	topicDescriber TopicDescriber,
	writerCfg *topicwriterinternal.WriterReconnectorConfig,
	multiWriterCfg *MultiWriterConfig,
) (*MultiWriter, error) {
	if multiWriterCfg.ProducerIDPrefix == "" {
		multiWriterCfg.ProducerIDPrefix = uuid.NewString()
	}

	var (
		ctx, cancel = context.WithCancel(context.Background())
		background  = background.NewWorker(ctx, "topic multiwriter background")
		encoders    = topicwritercommon.NewMultiEncoder()
	)

	for codec, creator := range writerCfg.AdditionalEncoders {
		encoders.AddEncoder(codec, creator)
	}

	p := &MultiWriter{
		ctx:          ctx,
		cfg:          multiWriterCfg,
		writerCfg:    writerCfg,
		encoders:     encoders,
		orchestrator: newOrchestrator(ctx, cancel, topicDescriber, background, writerCfg, multiWriterCfg),
		background:   background,
	}

	p.background.Start("init main worker", func(ctx context.Context) {
		err := p.orchestrator.init()
		if err != nil {
			p.orchestrator.stopWithError(err)

			return
		}
	})

	return p, nil
}

func (p *MultiWriter) Write(ctx context.Context, messages []topicwriterinternal.PublicMessage) error {
	for _, msg := range messages {
		if err := p.orchestrator.pushMessage(ctx, message{
			MessageWithDataContent: topicwritercommon.NewMessageDataWithContent(msg, p.encoders),
		}); err != nil {
			return err
		}
	}

	if p.writerCfg.WaitServerAck {
		return p.orchestrator.flush(ctx)
	}

	return nil
}

func (p *MultiWriter) Close(ctx context.Context) error {
	if p.closed.Swap(true) {
		return ErrAlreadyClosed
	}

	defer func() {
		_ = p.orchestrator.writerPool.close(ctx)
	}()

	if err := p.orchestrator.flush(ctx); err != nil {
		return err
	}

	p.orchestrator.stop()
	if err := p.background.Close(ctx, nil); err != nil {
		return err
	}

	return p.orchestrator.getResultErr()
}

func (p *MultiWriter) Flush(ctx context.Context) error {
	return p.orchestrator.flush(ctx)
}

func (p *MultiWriter) WaitInit(ctx context.Context) error {
	return p.orchestrator.waitInitDone(ctx)
}

func (p *MultiWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{}, fmt.Errorf(
		"%w: do not use WaitInitInfo when writing to many partitions, use WaitInit instead",
		ErrNotImplemented,
	)
}
