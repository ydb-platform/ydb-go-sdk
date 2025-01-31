package xsql

import (
	"context"
	"database/sql/driver"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/legacy"
	propose "github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/propose"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ io.Closer        = (*Connector)(nil)
	_ driver.Connector = (*Connector)(nil)
)

type (
	Engine    uint8
	Connector struct {
		parent   ydbDriver
		balancer grpc.ClientConnInterface

		processor Engine

		LegacyOpts            []legacy.Option
		Options               []propose.Option
		disableServerBalancer bool
		onCLose               []func(*Connector)

		clock          clockwork.Clock
		idleThreshold  time.Duration
		conns          xsync.Map[uuid.UUID, *Conn]
		done           chan struct{}
		trace          *trace.DatabaseSQL
		traceRetry     *trace.Retry
		retryBudget    budget.Budget
		pathNormalizer bind.TablePathPrefix
		bindings       bind.Bindings
	}
	ydbDriver interface {
		Name() string
		Table() table.Client
		Query() *query.Client
		Scripting() scripting.Client
		Scheme() scheme.Client
	}
)

func (e Engine) String() string {
	switch e {
	case LEGACY:
		return "LEGACY"
	case PROPOSE:
		return "PROPOSE"
	default:
		return "UNKNOWN"
	}
}

func (c *Connector) RetryBudget() budget.Budget {
	return c.retryBudget
}

func (c *Connector) Bindings() bind.Bindings {
	return c.bindings
}

func (c *Connector) Clock() clockwork.Clock {
	return c.clock
}

func (c *Connector) Trace() *trace.DatabaseSQL {
	return c.trace
}

func (c *Connector) TraceRetry() *trace.Retry {
	return c.traceRetry
}

func (c *Connector) Query() *query.Client {
	return c.parent.Query()
}

func (c *Connector) Name() string {
	return c.parent.Name()
}

func (c *Connector) Table() table.Client {
	return c.parent.Table()
}

func (c *Connector) Scripting() scripting.Client {
	return c.parent.Scripting()
}

func (c *Connector) Scheme() scheme.Client {
	return c.parent.Scheme()
}

const (
	PROPOSE = iota + 1
	LEGACY
)

func (c *Connector) Open(name string) (driver.Conn, error) {
	return nil, xerrors.WithStackTrace(driver.ErrSkip)
}

func (c *Connector) Connect(ctx context.Context) (_ driver.Conn, finalErr error) { //nolint:funlen
	onDone := trace.DatabaseSQLOnConnectorConnect(c.Trace(), &ctx,
		stack.FunctionID("database/sql.(*Connector).Connect", stack.Package("database/sql")),
	)

	switch c.processor {
	case PROPOSE:
		s, err := query.CreateSession(ctx, c.Query())
		defer func() {
			onDone(s, finalErr)
		}()
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		id := uuid.New()

		conn := &Conn{
			processor: PROPOSE,
			cc: propose.New(ctx, c, s, append(
				c.Options,
				propose.WithOnClose(func() {
					c.conns.Delete(id)
				}))...,
			),
			ctx:       ctx,
			connector: c,
			lastUsage: xsync.NewLastUsage(xsync.WithClock(c.Clock())),
		}

		c.conns.Set(id, conn)

		return conn, nil

	case LEGACY:
		s, err := c.Table().CreateSession(ctx) //nolint:staticcheck
		defer func() {
			onDone(s, finalErr)
		}()
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		id := uuid.New()

		conn := &Conn{
			processor: LEGACY,
			cc: legacy.New(ctx, c, s, append(c.LegacyOpts,
				legacy.WithOnClose(func() {
					c.conns.Delete(id)
				}))...,
			),
			ctx:       ctx,
			connector: c,
			lastUsage: xsync.NewLastUsage(xsync.WithClock(c.Clock())),
		}

		c.conns.Set(id, conn)

		return conn, nil
	default:
		return nil, xerrors.WithStackTrace(errWrongQueryProcessor)
	}
}

func (c *Connector) Driver() driver.Driver {
	return c
}

func (c *Connector) Parent() ydbDriver {
	return c.parent
}

func (c *Connector) Close() error {
	select {
	case <-c.done:
		return xerrors.WithStackTrace(errAlreadyClosed)
	default:
		close(c.done)

		for _, onClose := range c.onCLose {
			onClose(c)
		}

		return nil
	}
}

func Open(parent ydbDriver, balancer grpc.ClientConnInterface, opts ...Option) (_ *Connector, err error) {
	c := &Connector{
		parent:   parent,
		balancer: balancer,
		processor: func() Engine {
			if overQueryService, _ := strconv.ParseBool(os.Getenv("YDB_DATABASE_SQL_OVER_QUERY_SERVICE")); overQueryService {
				return PROPOSE
			}

			return LEGACY
		}(),
		clock:          clockwork.NewRealClock(),
		done:           make(chan struct{}),
		trace:          &trace.DatabaseSQL{},
		traceRetry:     &trace.Retry{},
		pathNormalizer: bind.TablePathPrefix(parent.Name()),
	}

	for _, opt := range opts {
		if opt != nil {
			if err = opt.Apply(c); err != nil {
				return nil, err
			}
		}
	}

	if c.idleThreshold > 0 {
		ctx, cancel := xcontext.WithDone(context.Background(), c.done)
		go func() {
			defer cancel()
			for {
				idleThresholdTimer := c.clock.NewTimer(c.idleThreshold)
				select {
				case <-ctx.Done():
					idleThresholdTimer.Stop()

					return
				case <-idleThresholdTimer.Chan():
					idleThresholdTimer.Stop() // no really need, stop for common style only
					c.conns.Range(func(_ uuid.UUID, cc *Conn) bool {
						if c.clock.Since(cc.LastUsage()) > c.idleThreshold {
							_ = cc.Close()
						}

						return true
					})
				}
			}
		}()
	}

	return c, nil
}
