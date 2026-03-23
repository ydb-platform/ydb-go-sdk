package topicmultiwriter

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type partitionWriterPool struct {
	ctx context.Context //nolint:containedctx

	cfg       *MultiWriterConfig
	writerCfg *topicwriterinternal.WriterReconnectorConfig
	bg        *background.Worker

	mu      xsync.Mutex
	writers map[int64]*writerWrapper
	idle    *idleWriterManager

	ackCallback            func(partitionID int64, seqNo int64)
	partitionSplitCallback func(partitionID int64)
	onWriterInit           func()
	onError                func(err error)
}

func newPartitionWriterPool(
	ctx context.Context,
	cfg *MultiWriterConfig,
	writerCfg *topicwriterinternal.WriterReconnectorConfig,
	bg *background.Worker,
	ackCallback func(partitionID int64, seqNo int64),
	partitionSplitCallback func(partitionID int64),
	onWriterInit func(),
	onError func(err error),
) *partitionWriterPool {
	p := &partitionWriterPool{
		cfg:                    cfg,
		writerCfg:              writerCfg,
		ctx:                    ctx,
		bg:                     bg,
		ackCallback:            ackCallback,
		partitionSplitCallback: partitionSplitCallback,
		onWriterInit:           onWriterInit,
		onError:                onError,
		writers:                make(map[int64]*writerWrapper),
		idle:                   newIdleWriterManager(ctx, cfg.WriterIdleTimeout),
	}

	bg.Start("idle-writer-manager", func(ctx context.Context) {
		p.idle.run()
	})

	return p
}

func (p *partitionWriterPool) getProducerID(partitionID int64) string {
	return fmt.Sprintf("%s-%d", p.cfg.ProducerIDPrefix, partitionID)
}

func (p *partitionWriterPool) createDirectWriter(partitionID int64) (writer, error) {
	withCustomCheckRetryErrorFunction := func(
		callback topic.PublicCheckErrorRetryFunction,
	) topicwriterinternal.PublicWriterOption {
		return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
			cfg.RetrySettings.CheckError = callback
		}
	}

	var (
		writerCfg = *p.writerCfg
		opts      = []topicwriterinternal.PublicWriterOption{
			topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithPartitionID(partitionID)),
			topicwriterinternal.WithProducerID(p.getProducerID(partitionID)),
			topicwriterinternal.WithAutoSetSeqNo(false),
			topicwriterinternal.WithOnAckReceivedCallback(func(seqNo int64) {
				p.ackCallback(partitionID, seqNo)
			}),
			withCustomCheckRetryErrorFunction(func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
				if isOperationErrorOverloaded(args.Error) {
					p.partitionSplitCallback(partitionID)

					return topic.PublicRetryDecisionStop
				}

				var checkErrorResult topic.PublicCheckRetryResult
				p.mu.WithLock(func() {
					if p.writerCfg.RetrySettings.CheckError != nil {
						checkErrorResult = p.writerCfg.RetrySettings.CheckError(args)
					}
				})

				return checkErrorResult
			}),
			topicwriterinternal.WithMaxQueueLen(p.writerCfg.MaxQueueLen),
		}
	)

	writerCfg.MultiMode = true
	for _, opt := range opts {
		opt(&writerCfg)
	}

	wr, err := p.cfg.writersFactory.Create(writerCfg)
	if err != nil {
		return nil, err
	}

	return wr, nil
}

func (p *partitionWriterPool) createNonDirectWriter(partitionID int64) (writer, error) {
	writerCfg := *p.writerCfg
	writerCfg.MultiMode = true
	topicwriterinternal.WithProducerID(p.getProducerID(partitionID))(&writerCfg)
	writer, err := p.cfg.writersFactory.Create(writerCfg)

	return writer, err
}

func (p *partitionWriterPool) get(partitionID int64, direct bool, doNotCreate bool) (*writerWrapper, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	finish := func() (*writerWrapper, error) {
		if doNotCreate {
			return nil, nil //nolint:nilnil
		}

		return p.createNewWriter(partitionID, direct)
	}

	existingWriter, ok := p.writers[partitionID]
	if ok {
		if existingWriter.direct != direct {
			p.forceEvictNeedLock(partitionID)

			return finish()
		}

		return existingWriter, nil
	}

	idleWriter, ok := p.idle.getWriterIfExists(partitionID)
	if ok {
		if idleWriter.direct != direct {
			_ = idleWriter.Close(p.ctx)

			return finish()
		}

		p.writers[partitionID] = idleWriter

		return idleWriter, nil
	}

	return finish()
}

func (p *partitionWriterPool) createNewWriter(partitionID int64, direct bool) (*writerWrapper, error) {
	var (
		wr  writer
		err error
	)

	if direct {
		wr, err = p.createDirectWriter(partitionID)
		if err != nil {
			return nil, err
		}
	} else {
		wr, err = p.createNonDirectWriter(partitionID)
		if err != nil {
			return nil, err
		}
	}

	wrapper := &writerWrapper{
		writer: wr,
		direct: direct,
	}
	p.writers[partitionID] = wrapper
	if !direct {
		return wrapper, nil
	}

	p.bg.Start(fmt.Sprintf("writer-init-%d", partitionID), func(ctx context.Context) {
		_, err := wr.WaitInitInfo(ctx)
		if err != nil {
			wrapper.err = err
		}

		wrapper.initDone.Store(true)
		p.onWriterInit()
	})

	return wrapper, nil
}

func (p *partitionWriterPool) forceEvict(partitionID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.forceEvictNeedLock(partitionID)
}

func (p *partitionWriterPool) evict(partitionID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	writer, ok := p.writers[partitionID]
	if !ok {
		return
	}

	delete(p.writers, partitionID)

	if !writer.direct {
		_ = writer.Close(p.ctx)

		return
	}

	p.idle.addWriter(partitionID, writer)
	p.idle.wakeup()
}

func (p *partitionWriterPool) forceEvictNeedLock(partitionID int64) {
	writer, ok := p.writers[partitionID]
	if !ok {
		return
	}

	delete(p.writers, partitionID)
	_ = writer.Close(p.ctx)
}

func (p *partitionWriterPool) close(ctx context.Context) error {
	var writersToClose []writer

	p.mu.WithLock(func() {
		writersToClose = make([]writer, 0, len(p.writers))
		for _, writer := range p.writers {
			writersToClose = append(writersToClose, writer)
		}
	})

	for _, writer := range writersToClose {
		if err := writer.Close(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (p *partitionWriterPool) getWritersCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.idle.getWritersCount() + len(p.writers)
}
