package pool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/node"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
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
		warmUpItems        int
	}
	itemInfo[PT ItemConstraint[T], T any] struct {
		item       PT
		created    time.Time
		lastUsage  time.Time
		useCounter uint64
	}
	Pool[PT ItemConstraint[T], T any] struct {
		config *Config[PT, T]

		stats *xsync.Value[dynamicStats]

		sema chan struct{}
		idle container[PT, T]

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

func WithWarmUpItems[PT ItemConstraint[T], T any](size int) Option[PT, T] {
	return func(c *Config[PT, T]) {
		if size > 0 {
			c.warmUpItems = size
		}
	}
}

func New[PT ItemConstraint[T], T any](
	ctx context.Context,
	opts ...Option[PT, T],
) (_ *Pool[PT, T], err error) {
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

	p.stats = xsync.NewValue(dynamicStats{})

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

	if err = p.warmUp(ctx); err != nil {
		_ = p.Close(ctx)

		return nil, xerrors.WithStackTrace(err)
	}

	return p, nil
}

func (p *Pool[PT, T]) warmUp(ctx context.Context) error {
	n := p.config.warmUpItems
	if n <= 0 {
		return nil
	}
	if n > p.config.limit {
		n = p.config.limit
	}

	for range n {
		if err := ctx.Err(); err != nil {
			return xerrors.WithStackTrace(ctx.Err())
		}

		item, err := p.createItem(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		if err := p.idle.Put(&itemInfo[PT, T]{
			item:    item,
			created: p.config.clock.Now(),
		}); err != nil {
			p.closeItem(ctx, item)

			return xerrors.WithStackTrace(err)
		}
	}

	p.changeStats(func(old dynamicStats) dynamicStats {
		old.Size = n
		old.Idle = n

		return old
	})

	return nil
}

// createItem wraps the Config.createItemFunc function with timeout handling
//
// Given context restrictions (timeout, cancel) ignored
func (p *Pool[PT, T]) createItem(ctx context.Context) (PT, error) {
	p.changeStats(func(old dynamicStats) dynamicStats {
		old.CreateInProgress++

		return old
	})
	defer func() {
		p.changeStats(func(old dynamicStats) dynamicStats {
			old.CreateInProgress--

			return old
		})
	}()

	createCtx, cancelCreate := xcontext.WithDone(xcontext.ValueOnly(ctx), p.done)
	defer cancelCreate()

	if d := p.config.createTimeout; d > 0 {
		createCtx, cancelCreate = context.WithTimeout(createCtx, d)
		defer cancelCreate()
	}

	item, err := p.config.createItemFunc(createCtx)
	if err != nil {
		return nil, err
	}

	if item == nil {
		return nil, errNilItem
	}

	p.changeStats(func(old dynamicStats) dynamicStats {
		old.Size++

		return old
	})

	return item, nil
}

// closeItem wraps the Config.closeItemFunc function with timeout handling
func (p *Pool[PT, T]) closeItem(ctx context.Context, item PT) {
	defer func() {
		p.changeStats(func(old dynamicStats) dynamicStats {
			old.Size--

			return old
		})
	}()

	closeCtx, cancelClose := xcontext.WithDone(xcontext.ValueOnly(ctx), p.done)
	defer cancelClose()

	if d := p.config.closeTimeout; d > 0 {
		closeCtx, cancelClose = context.WithTimeout(closeCtx, d)
		defer cancelClose()
	}

	p.config.closeItemFunc(closeCtx, item)
}

func (p *Pool[PT, T]) Stats() Stats {
	return Stats{
		dynamicStats: p.stats.Get(),
		Limit:        p.config.limit,
		WarmUp:       p.config.warmUpItems,
	}
}

func (p *Pool[PT, T]) changeStats(f func(old dynamicStats) dynamicStats) {
	onChange := p.config.trace.OnChange

	var stats dynamicStats
	p.stats.Change(func(old dynamicStats) dynamicStats {
		stats = f(old)

		return stats
	})

	if onChange != nil {
		onChange(Stats{
			dynamicStats: stats,
			Limit:        p.config.limit,
			WarmUp:       p.config.warmUpItems,
		})
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

//nolint:funlen
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
			p.sema <- struct{}{}
		}()
	}

	info, err := p.getItem(ctx)
	if err != nil {
		if isRetriable(err) {
			return xerrors.WithStackTrace(xerrors.Retryable(err))
		}

		if xerrors.IsContextError(err) && ctx.Err() == nil {
			return xerrors.WithStackTrace(xerrors.Retryable(err))
		}

		return xerrors.WithStackTrace(err)
	}

	p.changeStats(func(old dynamicStats) dynamicStats {
		old.InUse++

		return old
	})

	defer func() {
		p.changeStats(func(old dynamicStats) dynamicStats {
			old.InUse--

			return old
		})

		if err := p.checkItemAndError(info.item, finalErr); err != nil {
			p.closeItem(ctx, info.item)

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
	p.changeStats(func(old dynamicStats) dynamicStats {
		old.Concurrency++

		return old
	})
	defer func() {
		p.changeStats(func(old dynamicStats) dynamicStats {
			old.Concurrency--

			return old
		})
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
				p.closeItem(ctx, info.item)
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

func (p *Pool[PT, T]) popItem(nodeID uint32, useNodeID bool) (info *itemInfo[PT, T], _ error) {
	defer func() {
		if info != nil {
			p.changeStats(func(old dynamicStats) dynamicStats {
				old.Idle--

				return old
			})
		}
	}()

	if useNodeID {
		return p.idle.PopByNodeID(nodeID)
	}

	return p.idle.Pop()
}

// getItem called only under p.sema lock
func (p *Pool[PT, T]) getItem(ctx context.Context) (info *itemInfo[PT, T], finalErr error) {
	nodeID, hasPreferredNodeID := endpoint.ContextNodeID(ctx)

	if onGet := p.config.trace.OnGet; onGet != nil {
		onDone := onGet(&ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).getItem"),
		)
		if onDone != nil {
			defer func() {
				var item PT
				if info != nil {
					item = info.item
				}
				onDone(item, getNodeHintInfo(item, nodeID, hasPreferredNodeID, finalErr), finalErr)
			}()
		}
	}

	for range 2 {
		info, err := p.popItem(nodeID, hasPreferredNodeID)
		if err != nil {
			if xerrors.Is(err, errNothingIdleItems) {
				break
			}
		} else {
			if info.item.IsAlive() && !needCloseItem(p.config, info) {
				return info, nil
			}
			p.closeItem(ctx, info.item)
		}
	}

	if hasPreferredNodeID && p.idle.Len() >= p.config.limit { // race between Len and Pop
		// clear one slot in p.idle for create item with predefined nodeID later
		info, err := p.idle.Pop()
		if err == nil {
			p.closeItem(ctx, info.item)
		}
	}

	// create item after two fails
	item, err := p.createItem(ctx)
	if err != nil {
		if isRetriable(err) {
			return nil, xerrors.Retryable(err)
		}
		if xerrors.IsYdb(err) {
			return nil, err
		}

		return nil, err
	}

	if item == nil {
		return nil, errNilItem
	}

	return &itemInfo[PT, T]{
		item:       item,
		created:    p.config.clock.Now(),
		useCounter: 0,
	}, nil
}

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
		p.closeItem(ctx, info.item)

		return xerrors.WithStackTrace(errClosedPool)
	default:
		info.useCounter++
		info.lastUsage = p.config.clock.Now()

		if err := p.idle.Put(info); err != nil {
			p.closeItem(ctx, info.item)

			return err
		}

		if info != nil {
			p.changeStats(func(old dynamicStats) dynamicStats {
				old.Idle++

				return old
			})
		}

		return nil
	}
}
