package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/node"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	Item interface {
		closer.Closer
		node.ID

		IsAlive() bool
	}
	ItemConstraint[T any] interface {
		*T
		Item
	}
	Config[PT ItemConstraint[T], T any] struct {
		trace              *Trace
		clock              clockwork.Clock
		limit              int
		createTimeout      time.Duration
		createItemFunc     func(ctx context.Context) (PT, error)
		mustDeleteItemFunc func(item PT, err error) bool
		closeTimeout       time.Duration
		closeItemFunc      func(ctx context.Context, item PT)
		idleTimeToLive     time.Duration
		itemUsageLimit     uint64
		itemUsageTTL       time.Duration
	}
	itemInfo[PT ItemConstraint[T], T any] struct {
		item       PT
		created    time.Time
		lastUsage  time.Time
		useCounter uint64
	}
	Pool[PT ItemConstraint[T], T any] struct {
		config *Config[PT, T]

		createItemFunc   func(ctx context.Context) (PT, error)
		createInProgress xsync.Value[int]

		sema chan struct{}
		idle container[PT, T]

		concurrency atomic.Int64

		done chan struct{}
	}
	Option[PT ItemConstraint[T], T any] func(c *Config[PT, T])
)

func WithCreateItemFunc[PT ItemConstraint[T], T any](f func(ctx context.Context) (PT, error)) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.createItemFunc = f
	}
}

func WithMustDeleteItemFunc[PT ItemConstraint[T], T any](f func(item PT, err error) bool) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.mustDeleteItemFunc = f
	}
}

func WithCreateItemTimeout[PT ItemConstraint[T], T any](t time.Duration) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.createTimeout = t
	}
}

func WithCloseItemTimeout[PT ItemConstraint[T], T any](t time.Duration) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.closeTimeout = t
	}
}

func WithLimit[PT ItemConstraint[T], T any](size int) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.limit = size
	}
}

func WithItemUsageLimit[PT ItemConstraint[T], T any](itemUsageLimit uint64) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.itemUsageLimit = itemUsageLimit
	}
}

func WithItemUsageTTL[PT ItemConstraint[T], T any](ttl time.Duration) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.itemUsageTTL = ttl
	}
}

func WithTrace[PT ItemConstraint[T], T any](t *Trace) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.trace = t
	}
}

func WithIdleTimeToLive[PT ItemConstraint[T], T any](idleTTL time.Duration) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.idleTimeToLive = idleTTL
	}
}

func WithClock[PT ItemConstraint[T], T any](clock clockwork.Clock) Option[PT, T] {
	return func(c *Config[PT, T]) {
		c.clock = clock
	}
}

func New[PT ItemConstraint[T], T any](
	ctx context.Context,
	opts ...Option[PT, T],
) *Pool[PT, T] {
	p := &Pool[PT, T]{
		config: &Config[PT, T]{
			trace: &Trace{},
			clock: clockwork.NewRealClock(),
			limit: DefaultLimit,
			createItemFunc: func(ctx context.Context) (PT, error) {
				var item T

				return &item, nil
			},
			closeItemFunc: func(ctx context.Context, item PT) {
				_ = item.Close(ctx)
			},
			createTimeout: defaultCreateTimeout,
			closeTimeout:  defaultCloseTimeout,
			mustDeleteItemFunc: func(item PT, err error) bool {
				return !item.IsAlive()
			},
		},
		idle: &sliceContainer[PT, T]{},
		done: make(chan struct{}),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(p.config)
		}
	}

	if onNew := p.config.trace.OnNew; onNew != nil {
		onDone := onNew(&ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.New"),
		)
		if onDone != nil {
			defer func() {
				onDone(p.config.limit)
			}()
		}
	}

	p.sema = make(chan struct{}, p.config.limit)

	for range p.config.limit {
		p.sema <- struct{}{}
	}

	p.createItemFunc = makeAsyncCreateItemFunc(p)

	return p
}

// makeAsyncCreateItemFunc wraps the createItem function with timeout handling
func makeAsyncCreateItemFunc[PT ItemConstraint[T], T any]( //nolint:funlen
	p *Pool[PT, T],
) func(ctx context.Context) (PT, error) {
	return func(ctx context.Context) (PT, error) {
		var haveSlot bool
		p.createInProgress.Change(func(old int) int {
			if p.idle.Len()+old > p.config.limit {
				return old
			}

			haveSlot = true

			return old + 1
		})

		if !haveSlot {
			return nil, xerrors.WithStackTrace(errPoolIsOverflow)
		}

		ch := make(chan PT)

		go func() {
			defer p.createInProgress.Change(func(old int) int {
				close(ch)

				return old - 1
			})

			createCtx, cancel := xcontext.WithDone(xcontext.ValueOnly(ctx), p.done)
			defer cancel()

			if d := p.config.createTimeout; d > 0 {
				createCtx, cancel = context.WithTimeout(createCtx, d)
				defer cancel()
			}

			item, err := p.config.createItemFunc(createCtx)
			if err != nil {
				return
			}

			if item == nil {
				return
			}

			select {
			case <-ctx.Done():
				if err := p.idle.Put(&itemInfo[PT, T]{
					item:       item,
					created:    p.config.clock.Now(),
					lastUsage:  p.config.clock.Now(),
					useCounter: 0,
				}); err != nil {
					p.config.closeItemFunc(ctx, item)
				}
			case <-p.done:
				p.config.closeItemFunc(ctx, item)
			case ch <- item:
			}
		}()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, errClosedPool
		case item, has := <-ch:
			if !has {
				return nil, errNoProgress
			}
			if item == nil {
				return nil, errNilItem
			}

			return item, nil
		}
	}
}

func (p *Pool[PT, T]) Stats() Stats {
	return Stats{
		Limit:            p.config.limit,
		Idle:             p.idle.Len(),
		Concurrency:      int(p.concurrency.Load()),
		CreateInProgress: p.createInProgress.Get(),
	}
}

func (p *Pool[PT, T]) changeState(changeState func() Stats) {
	if stats, onChange := changeState(), p.config.trace.OnChange; onChange != nil {
		onChange(stats)
	}
}

func (p *Pool[PT, T]) checkItemAndError(item PT, err error) error {
	if !item.IsAlive() {
		return errItemIsNotAlive
	}

	if err == nil {
		return nil
	}

	if p.config.mustDeleteItemFunc(item, err) {
		return err
	}

	if !xerrors.IsValid(err, item) {
		return err
	}

	return nil
}

func (p *Pool[PT, T]) try(ctx context.Context, f func(ctx context.Context, item PT) error) (finalErr error) {
	if onTry := p.config.trace.OnTry; onTry != nil {
		onDone := onTry(&ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try"),
		)
		if onDone != nil {
			defer func() {
				onDone(finalErr)
			}()
		}
	}

	info, err := p.getItem(ctx)
	if err != nil {
		if xerrors.IsYdb(err) {
			switch {
			case xerrors.IsOperationError(err, Ydb.StatusIds_UNAUTHORIZED):
				// https://github.com/ydb-platform/ydb-go-sdk/issues/1550
				// Avoid retrying UNAUTHORIZED errors.
				return xerrors.WithStackTrace(xerrors.Unretryable(err))
			default:
				return xerrors.WithStackTrace(xerrors.Retryable(err))
			}
		}

		return xerrors.WithStackTrace(err)
	}

	defer func() {
		info.useCounter++
		info.lastUsage = p.config.clock.Now()

		if err := p.checkItemAndError(info.item, finalErr); err != nil {
			p.config.closeItemFunc(ctx, info.item)

			return
		}

		_ = p.putItem(ctx, info)
	}()

	err = f(ctx, info.item)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (p *Pool[PT, T]) With(
	ctx context.Context,
	f func(ctx context.Context, item PT) error,
	opts ...retry.Option,
) (finalErr error) {
	p.concurrency.Add(1)
	defer func() {
		p.concurrency.Add(-1)
	}()

	var attempts int

	if onWith := p.config.trace.OnWith; onWith != nil {
		onDone := onWith(&ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With"),
		)
		if onDone != nil {
			defer func() {
				onDone(attempts, finalErr)
			}()
		}
	}

	select {
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())
	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)
	case _, ok := <-p.sema:
		if !ok {
			return xerrors.WithStackTrace(errClosedPool)
		}
		defer func() {
			select {
			case <-p.done:
			case p.sema <- struct{}{}:
			}
		}()
	}

	err := retry.Retry(ctx, func(ctx context.Context) error {
		attempts++
		err := p.try(ctx, f)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, opts...)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("pool.With failed with %d attempts: %w", attempts, err))
	}

	return nil
}

func (p *Pool[PT, T]) Close(ctx context.Context) (finalErr error) {
	if onClose := p.config.trace.OnClose; onClose != nil {
		onDone := onClose(&ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).Close"),
		)
		if onDone != nil {
			defer func() {
				onDone(finalErr)
			}()
		}
	}

	select {
	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)
	default:
		close(p.done)

		var waitLocks sync.WaitGroup
		waitLocks.Add(p.config.limit)
		for range p.config.limit {
			go func() {
				defer waitLocks.Done()
				<-p.sema
			}()
		}
		waitLocks.Wait()
		close(p.sema)

		var waitCloses sync.WaitGroup
		data := p.idle.PopAll()
		waitCloses.Add(len(data))
		for _, info := range data {
			go func() {
				defer waitCloses.Done()
				p.config.closeItemFunc(ctx, info.item)
			}()
		}
		waitCloses.Wait()

		return nil
	}
}

func needCloseItemByMaxUsage[PT ItemConstraint[T], T any](c *Config[PT, T], info *itemInfo[PT, T]) bool {
	if c.itemUsageLimit <= 0 {
		return false
	}
	if info.useCounter < c.itemUsageLimit {
		return false
	}

	return true
}

func needCloseItemByTTL[PT ItemConstraint[T], T any](c *Config[PT, T], info *itemInfo[PT, T]) bool {
	if c.itemUsageTTL <= 0 {
		return false
	}
	if c.clock.Since(info.created) < c.itemUsageTTL {
		return false
	}

	return true
}

func needCloseItemByIdleTTL[PT ItemConstraint[T], T any](c *Config[PT, T], info *itemInfo[PT, T]) bool {
	if c.idleTimeToLive <= 0 {
		return false
	}
	if c.clock.Since(info.lastUsage) < c.idleTimeToLive {
		return false
	}

	return true
}

func needCloseItem[PT ItemConstraint[T], T any](c *Config[PT, T], info *itemInfo[PT, T]) bool {
	if needCloseItemByMaxUsage(c, info) {
		return true
	}
	if needCloseItemByTTL(c, info) {
		return true
	}
	if needCloseItemByIdleTTL(c, info) {
		return true
	}

	return false
}

func getNodeHintInfo[PT ItemConstraint[T], T any](
	item PT,
	preferredNodeID uint32,
	hasPreferredNodeID bool,
	finalErr error,
) *trace.NodeHintInfo {
	if !hasPreferredNodeID || finalErr != nil {
		return nil
	}
	res := &trace.NodeHintInfo{
		PreferredNodeID: preferredNodeID,
	}
	if item != nil {
		res.SessionNodeID = item.NodeID()
	}

	return res
}

func (p *Pool[PT, T]) getItem(ctx context.Context) (info *itemInfo[PT, T], finalErr error) { //nolint:funlen
	preferredNodeID, hasPreferredNodeID := endpoint.ContextNodeID(ctx)

	if onGet := p.config.trace.OnGet; onGet != nil {
		onDone := onGet(&ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).getItem"),
		)
		if onDone != nil {
			defer func() {
				onDone(info.item, 0, getNodeHintInfo(info.item, preferredNodeID, hasPreferredNodeID, finalErr), finalErr)
			}()
		}
	}

	for range 2 {
		var info *itemInfo[PT, T]
		if hasPreferredNodeID {
			var err error
			info, err = p.idle.PopByNodeID(preferredNodeID)
			if err != nil {
				continue
			}
		} else {
			var err error
			info, err = p.idle.PopAny()
			if err != nil {
				continue
			}
		}
		if info != nil {
			switch {
			case needCloseItem(p.config, info), !info.item.IsAlive():
				p.config.closeItemFunc(ctx, info.item)
			default:
				return info, nil
			}
		}
	}

	item, err := p.createItemFunc(ctx)
	if err != nil && isRetriable(err) {
		return nil, xerrors.WithStackTrace(xerrors.Retryable(err))
	}

	if item == nil {
		return nil, xerrors.WithStackTrace(errNilItem)
	}

	return &itemInfo[PT, T]{
		item:       item,
		created:    p.config.clock.Now(),
		lastUsage:  p.config.clock.Now(),
		useCounter: 0,
	}, nil
}

// p.mu must be free.
func (p *Pool[PT, T]) putItem(ctx context.Context, info *itemInfo[PT, T]) (finalErr error) {
	if onPut := p.config.trace.OnPut; onPut != nil {
		onDone := onPut(&ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).putItem"),
			info.item,
		)
		if onDone != nil {
			defer func() {
				onDone(finalErr)
			}()
		}
	}

	select {
	case <-p.done:
		p.config.closeItemFunc(ctx, info.item)

		return xerrors.WithStackTrace(errClosedPool)
	default:
		if err := p.idle.Put(info); err != nil {
			p.config.closeItemFunc(ctx, info.item)

			return err
		}

		return nil
	}
}
