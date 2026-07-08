package query

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// SharedSessionPoolConfig configures the driver-level session pool shared by table and query clients.
type SharedSessionPoolConfig struct {
	Limit                  int
	WarmUp                 int
	ItemUsageLimit         uint64
	ItemUsageTTL           time.Duration
	IdleTTL                time.Duration
	SessionCreateTimeout   time.Duration
	SessionDeleteTimeout   time.Duration
	DisableSessionBalancer bool
	Trace                  *trace.Query
}

// SessionPool is a driver-level pool of query session cores (CreateSession + AttachSession).
// Table and query clients lease sessions from the same pool instance.
type SessionPool struct {
	corePool *pool.Pool[*sessionCore, sessionCore]

	closed *xsync.Value[*closeState]
}

// NewSharedSessionPool creates and optionally warms up the driver session pool.
func NewSharedSessionPool( //nolint:funlen
	ctx context.Context,
	cc grpc.ClientConnInterface,
	cfg SharedSessionPoolConfig,
) (*SessionPool, error) {
	p := &SessionPool{
		closed: xsync.NewValue(&closeState{
			cancels: make(map[uint64]context.CancelFunc),
		}),
	}

	client := Ydb_Query_V1.NewQueryServiceClient(cc)
	traceQuery := cfg.Trace
	if traceQuery == nil {
		traceQuery = &trace.Query{}
	}

	deleteTimeout := cfg.SessionDeleteTimeout
	if deleteTimeout <= 0 {
		deleteTimeout = config.DefaultSessionDeleteTimeout
	}

	createTimeout := cfg.SessionCreateTimeout
	if createTimeout <= 0 {
		createTimeout = config.DefaultSessionCreateTimeout
	}

	limit := cfg.Limit
	if limit <= 0 {
		limit = config.DefaultPoolMaxSize
	}

	poolOpts := []pool.Option[*sessionCore, sessionCore]{
		pool.WithLimit[*sessionCore, sessionCore](limit),
		pool.WithWarmUpItems[*sessionCore, sessionCore](cfg.WarmUp),
		pool.WithItemUsageLimit[*sessionCore, sessionCore](cfg.ItemUsageLimit),
		pool.WithTrace[*sessionCore, sessionCore](sharedPoolTrace(traceQuery)),
		pool.WithCreateItemTimeout[*sessionCore, sessionCore](createTimeout),
		pool.WithCloseItemTimeout[*sessionCore, sessionCore](deleteTimeout),
		pool.WithMustDeleteItemFunc(func(s *sessionCore, err error) bool {
			if !s.IsAlive() {
				return true
			}

			return err != nil && xerrors.MustDeleteTableOrQuerySession(err)
		}),
		pool.WithCreateItemFunc(func(ctx context.Context) (*sessionCore, error) {
			if !cfg.DisableSessionBalancer {
				ctx = meta.WithAllowFeatures(ctx, meta.HintSessionBalancer)
			}

			return Open(ctx, client,
				WithConn(cc),
				WithDeleteTimeout(deleteTimeout),
				WithRegisterCloseCancel(p.registerCloseCancel),
				WithTrace(traceQuery),
			)
		}),
	}

	if cfg.ItemUsageTTL > 0 {
		poolOpts = append(poolOpts,
			pool.WithItemUsageTTL[*sessionCore, sessionCore](cfg.ItemUsageTTL),
		)
	}
	if cfg.IdleTTL > 0 {
		poolOpts = append(poolOpts,
			pool.WithIdleTimeToLive[*sessionCore, sessionCore](cfg.IdleTTL),
		)
	}

	corePool, err := pool.New(ctx, poolOpts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	p.corePool = corePool

	return p, nil
}

func (p *SessionPool) registerCloseCancel(cancel context.CancelFunc) func() {
	var id uint64
	p.closed.Change(func(old *closeState) *closeState {
		if old.closed {
			return old
		}

		new := *old

		new.nextCancelID++
		id = new.nextCancelID
		new.cancels[id] = cancel

		return &new
	})
	if id == 0 {
		cancel()

		return func() {}
	}

	return func() {
		p.unregisterCloseCancel(id)
	}
}

func (p *SessionPool) unregisterCloseCancel(id uint64) {
	p.closed.Change(func(old *closeState) *closeState {
		delete(old.cancels, id)

		return old
	})
}

// Stats returns pool counters (shared by table and query clients).
func (p *SessionPool) Stats() pool.Stats {
	return p.corePool.Stats()
}

// Close closes all pooled sessions.
func (p *SessionPool) Close(ctx context.Context) error {
	var cancels []context.CancelFunc
	p.closed.Change(func(old *closeState) *closeState {
		if old.closed {
			return old
		}

		old.closed = true
		cancels = make([]context.CancelFunc, 0, len(old.cancels))
		for _, cancel := range old.cancels {
			cancels = append(cancels, cancel)
		}
		old.cancels = nil

		return old
	})
	for _, cancel := range cancels {
		cancel()
	}

	if err := p.corePool.Close(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

// WithCore leases a session core from the pool.
func (p *SessionPool) WithCore(
	ctx context.Context,
	op func(ctx context.Context, core Core) error,
	opts ...retry.Option,
) error {
	return p.corePool.With(ctx, func(ctx context.Context, core *sessionCore) error {
		if err := op(ctx, core); err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, opts...)
}

// ClientFromCore returns the query gRPC client bound to a pooled session core.
func ClientFromCore(core Core) Ydb_Query_V1.QueryServiceClient {
	sc, ok := core.(*sessionCore)
	if !ok || sc == nil {
		return nil
	}

	return sc.Client
}

func sharedPoolTrace(t *trace.Query) *pool.Trace[*sessionCore, sessionCore] {
	return &pool.Trace[*sessionCore, sessionCore]{
		OnNew: func(ctx *context.Context, call stack.Caller) func(limit int) {
			onDone := gtrace.QueryOnPoolNew(t, ctx, call)

			return func(limit int) {
				onDone(limit)
			}
		},
		OnClose: func(ctx *context.Context, call stack.Caller) func(err error) {
			onDone := gtrace.QueryOnClose(t, ctx, call)

			return func(err error) {
				onDone(err)
			}
		},
		OnPut: func(ctx *context.Context, call stack.Caller, item *sessionCore) func(err error) {
			onDone := gtrace.QueryOnPoolPut(t, ctx, call, item)

			return func(err error) {
				onDone(err)
			}
		},
		OnGet: func(ctx *context.Context, call stack.Caller) func(
			session *sessionCore,
			hintInfo *trace.NodeHintInfo,
			attempts int,
			err error,
		) {
			onDone := gtrace.QueryOnPoolGet(t, ctx, call)

			return func(session *sessionCore, hintInfo *trace.NodeHintInfo, attempts int, err error) {
				onDone(session, attempts, hintInfo, err)
			}
		},
		OnWith: func(ctx *context.Context, call stack.Caller) func(attempts int, err error) {
			onDone := gtrace.QueryOnPoolWith(t, ctx, call)

			return func(attempts int, err error) {
				onDone(attempts, err)
			}
		},
		OnChange: func(stats pool.Stats) {
			gtrace.QueryOnPoolChange(t,
				stats.Limit, stats.Idle, stats.CreateInProgress, stats.Concurrency, stats.Size,
			)
		},
	}
}
