package conn

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func noopStopUsage() {}

func (c *conn) startUsage() func() {
	if c.usage == nil {
		return noopStopUsage
	}

	return c.usage.start()
}

func (c *conn) parkIfIdle(ctx context.Context, ttl time.Duration) {
	if c.usage == nil {
		return
	}

	c.usage.ifIdle(ttl, func() {
		s := c.State()
		if s == state.Online || s == state.Banned {
			_ = c.park(ctx)
		}
	})
}

func (c *conn) park(ctx context.Context) (err error) {
	onDone := gtrace.DriverOnConnPark(
		c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*conn).park"),
		c.Endpoint(),
	)
	defer func() {
		onDone(err)
	}()

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.closed || c.grpcConn == nil {
		return nil
	}

	if err = c.close(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}
