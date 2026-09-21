package topicmultiwriter

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
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

	ackCallback  func(partitionID int64, seqNo int64)
	source       *partition.Source
	onWriterInit func()
	onError      func(err error)
}

func newPartitionWriterPool(
	ctx context.Context,
	cfg *MultiWriterConfig,
	writerCfg *topicwriterinternal.WriterReconnectorConfig,
	bg *background.Worker,
	ackCallback func(partitionID int64, seqNo int64),
	source *partition.Source,
	onWriterInit func(),
	onError func(err error),
) *partitionWriterPool {
	p := &partitionWriterPool{
		cfg:          cfg,
		writerCfg:    writerCfg,
		ctx:          ctx,
		bg:           bg,
		ackCallback:  ackCallback,
		source:       source,
		onWriterInit: onWriterInit,
		onError:      onError,
		writers:      make(map[int64]*writerWrapper),
		idle:         newIdleWriterManager(ctx, cfg.WriterIdleTimeout),
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
				if p.source.NotifySessionError(p.ctx, partitionID, args.Error) != nil {
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
			topicwriterinternal.WithWaitAckOnWrite(false),
			topicwriterinternal.WithMaxQueueLen(p.writerCfg.MaxQueueLen),
		}
	)

	writerCfg.MultiMode = true
	for _, opt := range opts {
		opt(&writerCfg)
	}
	if p.cfg.DirectWrite {
		topicwriterinternal.WithDirectWrite(true)(&writerCfg)
	}

	wr, err := p.cfg.writersFactory.Create(writerCfg)
	if err != nil {
		return nil, err
	}

	return wr, nil
}

// readSeqNo uses a temporary session outside the working pool. Its errors return to the caller,
// which may report a split hint to Source after the session has closed.
func (p *partitionWriterPool) readSeqNo(ctx context.Context, partitionID int64, active bool) (int64, error) {
	writerCfg := *p.writerCfg
	writerCfg.MultiMode = true
	topicwriterinternal.WithProducerID(p.getProducerID(partitionID))(&writerCfg)
	if active {
		topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithPartitionID(partitionID))(&writerCfg)
		topicwriterinternal.WithDirectWrite(p.cfg.DirectWrite)(&writerCfg)
	}
	// Initialization owns retries; a seqNo session returns its own errors without split handling.
	writerCfg.RetrySettings.CheckError = func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		return topic.PublicRetryDecisionStop
	}

	wr, err := p.cfg.writersFactory.Create(writerCfg)
	if err != nil {
		return 0, err
	}
	defer func() { _ = wr.Close(ctx) }()

	info, err := wr.WaitInitInfo(ctx)
	if err != nil {
		return 0, err
	}

	return info.LastSeqNum, nil
}

func (p *partitionWriterPool) get(partitionID int64) (*writerWrapper, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existingWriter, ok := p.writers[partitionID]; ok {
		return existingWriter, nil
	}

	if idleWriter, ok := p.idle.getWriterIfExists(partitionID); ok {
		p.writers[partitionID] = idleWriter

		return idleWriter, nil
	}

	return p.createNewWriter(partitionID)
}

func (p *partitionWriterPool) createNewWriter(partitionID int64) (*writerWrapper, error) {
	wr, err := p.createDirectWriter(partitionID)
	if err != nil {
		return nil, err
	}

	wrapper := &writerWrapper{writer: wr}
	p.writers[partitionID] = wrapper
	p.bg.Start(fmt.Sprintf("writer-init-%d", partitionID), func(ctx context.Context) {
		_, err := wr.WaitInitInfo(ctx)
		wrapper.setInitErr(err)

		wrapper.initDone.Store(true)
		p.onWriterInit()
	})

	return wrapper, nil
}

func (p *partitionWriterPool) forceEvict(partitionID int64) {
	var writer *writerWrapper
	p.mu.WithLock(func() {
		writer = p.writers[partitionID]
		delete(p.writers, partitionID)
		if writer == nil {
			writer, _ = p.idle.getWriterIfExists(partitionID)
		}
	})
	if writer != nil {
		_ = writer.Close(p.ctx)
	}
}

func (p *partitionWriterPool) evict(partitionID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	writer, ok := p.writers[partitionID]
	if !ok {
		return
	}

	delete(p.writers, partitionID)
	p.idle.addWriter(partitionID, writer)
	p.idle.wakeup()
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

	return p.idle.close(ctx)
}

func (p *partitionWriterPool) getWritersCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.idle.getWritersCount() + len(p.writers)
}
