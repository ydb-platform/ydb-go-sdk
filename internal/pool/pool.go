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
		idle itemsContainer[PT, T]

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

func WithLimit[PT ItemConstraint[T], T any](limit int) Option[PT, T] {
	return func(c *Config[PT, T]) {
		if limit <= 0 {
			panic(fmt.Errorf("wrong limit value: %d", limit))
		}
		c.limit = limit
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

//nolint:funlen
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

	var batchChanges dynamicStats
	defer p.applyBatchStats(&batchChanges)

	if err = p.warmUp(ctx, &batchChanges); err != nil {
		_ = p.Close(ctx)

		return nil, xerrors.WithStackTrace(err)
	}

	return p, nil
}

func (p *Pool[PT, T]) warmUp(ctx context.Context, batchChanges *dynamicStats) error {
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

		item, err := p.createItem(ctx, batchChanges)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		now := p.config.clock.Now()
		if err := p.idle.Put(&itemInfo[PT, T]{
			item:      item,
			created:   now,
			lastUsage: now,
		}); err != nil {
			p.closeItem(ctx, item, batchChanges)

			return xerrors.WithStackTrace(err)
		}

		batchChanges.Idle++
	}

	return nil
}

// createItem wraps Config.createItemFunc with pool-controlled context handling.
//
// Caller context values are preserved, but caller cancellation and deadlines are
// not propagated. Creation is canceled when the pool is done, and
// Config.createTimeout is applied when configured.
func (p *Pool[PT, T]) createItem(ctx context.Context, batchChanges *dynamicStats) (PT, error) {
	batchChanges.CreateInProgress++
	defer func() {
		batchChanges.CreateInProgress--
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

	batchChanges.Size++

	return item, nil
}

// closeItem wraps the Config.closeItemFunc function with timeout handling
func (p *Pool[PT, T]) closeItem(ctx context.Context, item PT, batchChanges *dynamicStats) {
	defer func() {
		if batchChanges != nil {
			batchChanges.Size--
		}
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
func (p *Pool[PT, T]) try(ctx context.Context,
	f func(ctx context.Context, item PT) error, batchChanges *dynamicStats,
) (finalErr error) {
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

	info, err := p.getItem(ctx, batchChanges)
	if err != nil {
		if isRetriable(err) {
			return xerrors.WithStackTrace(xerrors.Retryable(err))
		}

		if xerrors.IsContextError(err) && ctx.Err() == nil {
			return xerrors.WithStackTrace(xerrors.Retryable(err))
		}

		return xerrors.WithStackTrace(err)
	}

	batchChanges.InUse++

	defer func() {
		batchChanges.InUse--

		if err := p.checkItemAndError(info.item, finalErr); err != nil {
			p.closeItem(ctx, info.item, batchChanges)

			return
		}

		_ = p.putItem(ctx, info, batchChanges)
	}()

	err = f(ctx, info.item)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (p *Pool[PT, T]) applyBatchStats(batch *dynamicStats) {
	onChange := p.config.trace.OnChange

	var stats dynamicStats
	p.stats.Change(func(old dynamicStats) dynamicStats {
		stats = old

		stats.Concurrency += batch.Concurrency
		stats.CreateInProgress += batch.CreateInProgress
		stats.InUse += batch.InUse
		stats.Idle += batch.Idle
		stats.Size += batch.Size

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

func (p *Pool[PT, T]) With(
	ctx context.Context,
	f func(ctx context.Context, item PT) error,
	opts ...retry.Option,
) (finalErr error) {
	p.stats.Change(func(old dynamicStats) dynamicStats {
		old.Concurrency++

		return old
	})

	var batchChanges dynamicStats
	defer func() {
		batchChanges.Concurrency--

		p.applyBatchStats(&batchChanges)
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
		err := p.try(ctx, f, &batchChanges)
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

		var (
			closes       sync.WaitGroup
			locks        sync.WaitGroup
			batchChanges dynamicStats
		)

		defer p.applyBatchStats(&batchChanges)

		locks.Add(p.config.limit)
		for range p.config.limit {
			go func() {
				defer locks.Done()
				<-p.sema
			}()
		}
		locks.Wait()
		close(p.sema)

		data := p.idle.Clear()
		batchChanges.Idle -= len(data)

		closes.Add(len(data))
		for _, info := range data {
			go func(info *itemInfo[PT, T]) {
				defer closes.Done()

				// nil batchChanges need for exclude data race on concurrent batchChanges changes
				p.closeItem(ctx, info.item, nil)
			}(info)
		}
		closes.Wait()
		batchChanges.Size -= len(data)

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

func (p *Pool[PT, T]) popItem(nodeID uint32, useNodeID bool, batchChanges *dynamicStats) (
	info *itemInfo[PT, T], _ error,
) {
	defer func() {
		if info != nil {
			batchChanges.Idle--
		}
	}()

	if useNodeID {
		return p.idle.PopByNodeID(nodeID)
	}

	return p.idle.Pop()
}

// getItem called only under p.sema lock
//
//nolint:funlen
func (p *Pool[PT, T]) getItem(ctx context.Context, batchChanges *dynamicStats) (info *itemInfo[PT, T], finalErr error) {
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
		info, err := p.popItem(nodeID, hasPreferredNodeID, batchChanges)
		if err != nil {
			if xerrors.Is(err, errNothingIdleItems) {
				break
			}
		} else {
			if info.item.IsAlive() && !needCloseItem(p.config, info) {
				return info, nil
			}
			p.closeItem(ctx, info.item, batchChanges)
		}
	}

	if hasPreferredNodeID {
		st := p.stats.Get()
		size := st.Size + batchChanges.Size
		if st.Concurrency == p.config.limit || size >= p.config.limit {
			// Free a slot before createItem: full concurrent load or pool already at limit.
			info, err := p.idle.Pop()
			if err != nil {
				return nil, errNothingIdleItems
			}

			p.closeItem(ctx, info.item, batchChanges)
		}
	}

	// create item after two fails
	item, err := p.createItem(ctx, batchChanges)
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

func (p *Pool[PT, T]) putItem(ctx context.Context, info *itemInfo[PT, T], batchChanges *dynamicStats) (finalErr error) {
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
		p.closeItem(ctx, info.item, batchChanges)

		return xerrors.WithStackTrace(errClosedPool)
	default:
		if p.idle.Len() >= p.config.limit {
			p.closeItem(ctx, info.item, batchChanges)

			return errPoolIsOverflow
		}

		info.useCounter++
		info.lastUsage = p.config.clock.Now()

		if err := p.idle.Put(info); err != nil {
			p.closeItem(ctx, info.item, batchChanges)

			return err
		}

		if info != nil {
			batchChanges.Idle++
		}

		return nil
	}
}
