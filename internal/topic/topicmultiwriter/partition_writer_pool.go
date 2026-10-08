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
	maxSeqNo  *seqNoCounter

	mu        xsync.Mutex
	writers   map[int64]*writerWrapper
	replacing map[int64]chan struct{}
	idle      *idleWriterManager

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
	maxSeqNo *seqNoCounter,
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
		maxSeqNo:               maxSeqNo,
		ackCallback:            ackCallback,
		partitionSplitCallback: partitionSplitCallback,
		onWriterInit:           onWriterInit,
		onError:                onError,
		writers:                make(map[int64]*writerWrapper),
		replacing:              make(map[int64]chan struct{}),
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

func (p *partitionWriterPool) createNonDirectWriter(partitionID int64) (writer, error) {
	writerCfg := *p.writerCfg
	writerCfg.MultiMode = true
	topicwriterinternal.WithProducerID(p.getProducerID(partitionID))(&writerCfg)
	writer, err := p.cfg.writersFactory.Create(writerCfg)

	return writer, err
}

//nolint:funlen
func (p *partitionWriterPool) get(partitionID int64, direct bool) (*writerWrapper, error) {
	for {
		var (
			result   *writerWrapper
			err      error
			old      writer
			wait     <-chan struct{}
			finished chan struct{}
		)
		p.mu.WithLock(func() {
			if err = p.ctx.Err(); err != nil {
				return
			}
			if wait = p.replacing[partitionID]; wait != nil {
				return
			}

			if existing := p.writers[partitionID]; existing != nil {
				if existing.direct == direct {
					result = existing

					return
				}
				delete(p.writers, partitionID)
				old = existing
			} else if idle, ok := p.idle.getWriterIfExists(partitionID); ok {
				if idle.direct == direct {
					p.writers[partitionID] = idle
					result = idle

					return
				}
				old = idle
			}
			if old == nil {
				result, err = p.createNewWriter(partitionID, direct)

				return
			}
			finished = make(chan struct{})
			p.replacing[partitionID] = finished
		})
		if wait != nil {
			select {
			case <-wait:
				continue
			case <-p.ctx.Done():
				return nil, p.ctx.Err()
			}
		}
		if old != nil {
			_ = old.Close(p.ctx)
			p.mu.WithLock(func() {
				if err = p.ctx.Err(); err == nil {
					result, err = p.createNewWriter(partitionID, direct)
				}
				delete(p.replacing, partitionID)
				close(finished)
			})
		}

		return result, err
	}
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
		writer:     wr,
		direct:     direct,
		initDoneCh: make(chan struct{}),
	}
	p.writers[partitionID] = wrapper
	if !direct {
		return wrapper, nil
	}

	p.bg.Start(fmt.Sprintf("writer-init-%d", partitionID), func(ctx context.Context) {
		info, err := wr.WaitInitInfo(ctx)
		if err == nil {
			p.maxSeqNo.advance(info.LastSeqNum)
		}
		wrapper.setInitErr(err)

		wrapper.initDone.Store(true)
		close(wrapper.initDoneCh)
		p.onWriterInit()
	})

	return wrapper, nil
}

func (p *partitionWriterPool) evict(partitionID int64) {
	if old := p.remove(partitionID); old != nil {
		_ = old.Close(p.ctx)
	}
}

func (p *partitionWriterPool) remove(partitionID int64) writer {
	var toClose writer
	p.mu.WithLock(func() {
		w := p.writers[partitionID]
		if w == nil {
			return
		}
		delete(p.writers, partitionID)
		if w.direct {
			p.idle.addWriter(partitionID, w)
			p.idle.wakeup()
		} else {
			toClose = w
		}
	})

	return toClose
}

func (p *partitionWriterPool) discard(partitionID int64, failed *writerWrapper) {
	p.mu.WithLock(func() {
		if p.writers[partitionID] != failed {
			failed = nil

			return
		}
		delete(p.writers, partitionID)
	})
	if failed != nil {
		_ = failed.Close(p.ctx)
	}
}

func (p *partitionWriterPool) close(ctx context.Context) error {
	var writersToClose []writer
	for {
		var replacing []<-chan struct{}
		p.mu.WithLock(func() {
			for _, ch := range p.replacing {
				replacing = append(replacing, ch)
			}
		})
		if len(replacing) == 0 {
			break
		}
		for _, ch := range replacing {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
			}
		}
	}

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
