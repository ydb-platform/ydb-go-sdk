package xsql

import (
	"context"
	"database/sql/driver"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	querySql "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn"
	tableSql "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
	queryProcessor uint8
	Connector      struct {
		parent   ydbDriver
		balancer grpc.ClientConnInterface

		queryProcessor queryProcessor

		tableOpts             []tableSql.Option
		queryOpts             []querySql.Option
		disableServerBalancer bool
		onCLose               []func(*Connector)

		clock         clockwork.Clock
		idleThreshold time.Duration
		conns         xsync.Map[uuid.UUID, *connWrapper]
		done          chan struct{}
		traceSql      *trace.DatabaseSQL
		traceRetry    *trace.Retry
		retryBudget   budget.Budget
	}
	ydbDriver interface {
		Name() string
		Table() table.Client
		Query() *query.Client
		Scripting() scripting.Client
		Scheme() scheme.Client
	}
)

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
	queryProcessor_QueryService = iota + 1
	queryProcessor_TableService
)

func (c *Connector) Open(name string) (driver.Conn, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	switch c.queryProcessor {
	case queryProcessor_QueryService:
		id := uuid.New()
		cc, err := querySql.New(ctx, c, append(
			c.queryOpts,
			querySql.WithClock(c.clock),
			querySql.WithOnClose(func() {
				c.conns.Delete(id)
			}))...,
		)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		conn := &connWrapper{
			conn:           cc,
			connector:      c,
			pathNormalizer: bind.TablePathPrefix(c.Name()),
		}

		c.conns.Set(id, conn)

		return conn, nil

	case queryProcessor_TableService:
		id := uuid.New()
		cc, err := tableSql.New(ctx, c, append(
			c.tableOpts,
			tableSql.WithClock(c.clock),
			tableSql.WithOnClose(func() {
				c.conns.Delete(id)
			}))...,
		)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		conn := &connWrapper{
			conn:           cc,
			connector:      c,
			pathNormalizer: bind.TablePathPrefix(c.Name()),
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
		parent:         parent,
		balancer:       balancer,
		queryProcessor: queryProcessor_TableService,
		clock:          clockwork.NewRealClock(),
		done:           make(chan struct{}),
		traceSql:       &trace.DatabaseSQL{},
		traceRetry:     &trace.Retry{},
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
					c.conns.Range(func(_ uuid.UUID, cc *connWrapper) bool {
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
