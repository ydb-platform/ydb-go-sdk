package table

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func TestSessionFromQueryCoreClose(t *testing.T) {
	core := newAdapterTestCore(1)
	s := sessionFromQueryCore(&adapterTestConn{}, config.New(), core)
	require.Equal(t, core.ID(), s.ID())
	require.Equal(t, core.NodeID(), s.NodeID())
	require.WithinDuration(t, time.Now(), s.LastUsage(), time.Second)
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	require.EqualValues(t, 1, core.closes.Load())
	require.False(t, s.IsAlive())
	require.Equal(t, table.SessionClosed, s.Status())
}

func TestSessionFromQueryCorePhysicalClose(t *testing.T) {
	core := newAdapterTestCore(1)
	s := sessionFromQueryCore(&adapterTestConn{}, config.New(), core)
	core.SetStatus(query.StatusClosing)
	require.False(t, s.IsAlive())
	require.Equal(t, table.SessionClosing, s.Status())
	core.SetStatus(query.StatusClosed)
	require.False(t, s.IsAlive())
	require.Equal(t, table.SessionClosed, s.Status())
}

func TestSessionFromQueryCoreTableMiddleware(t *testing.T) {
	core := newAdapterTestCore(1)
	calls := 0
	cc := &adapterTestConn{invoke: func(ctx context.Context) {
		calls++
		nodeID, ok := endpoint.ContextNodeID(ctx)
		require.True(t, ok)
		require.Equal(t, core.NodeID(), nodeID)
		meta.CallTrailerCallback(ctx, metadata.Pairs(meta.HeaderServerHints, meta.HintSessionClose))
	}}
	s := sessionFromQueryCore(cc, config.New(), core)
	s.lastUsage.Store(1)
	_, err := s.client.KeepAlive(context.Background(), &Ydb_Table.KeepAliveRequest{SessionId: s.ID()})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.WithinDuration(t, time.Now(), s.LastUsage(), time.Second)
	require.Equal(t, table.SessionClosing, s.Status())
	require.False(t, s.IsAlive())
}

func TestSessionFromQueryCoreExecutor(t *testing.T) {
	for _, overQuery := range []bool{false, true} {
		t.Run(map[bool]string{false: "table", true: "query"}[overQuery], func(t *testing.T) {
			core := newAdapterTestCore(1)
			s := sessionFromQueryCore(&adapterTestConn{}, config.New(config.ExecuteDataQueryOverQueryService(overQuery)), core)
			if overQuery {
				executor, ok := s.dataQuery.(queryClientExecutor)
				require.True(t, ok)
				require.Same(t, core, executor.core)
			} else {
				_, ok := s.dataQuery.(tableClientExecutor)
				require.True(t, ok)
			}
		})
	}
}

func TestSessionPoolAdapterCloseDoesNotCloseDriverPool(t *testing.T) {
	require.NoError(t, (&sessionPoolAdapter{}).Close(context.Background()))
}

func TestSharedTableAdapterReusesPhysicalSession(t *testing.T) {
	a, created := newAdapterTestPool(t, &adapterTestConn{})
	var borrowed *Session
	require.NoError(t, a.With(context.Background(), func(_ context.Context, s *Session) error {
		borrowed = s

		return nil
	}))
	require.NoError(t, a.Close(context.Background()))
	require.NoError(t, a.sessionPool.With(context.Background(), func(_ context.Context, s *query.Session) error {
		require.Same(t, borrowed.queryCore, s.Core)

		return nil
	}))
	require.EqualValues(t, 1, created.Load())
	require.Equal(t, 1, a.Stats().Idle)
}

func TestSharedTableAdapterDiscardsClosedSession(t *testing.T) {
	for _, closeMode := range []string{"close", "hint", "status"} {
		t.Run(closeMode, func(t *testing.T) {
			cc := &adapterTestConn{invoke: func(ctx context.Context) {
				meta.CallTrailerCallback(ctx, metadata.Pairs(meta.HeaderServerHints, meta.HintSessionClose))
			}}
			a, created := newAdapterTestPool(t, cc)
			var oldCore query.Core
			require.NoError(t, a.With(context.Background(), func(ctx context.Context, s *Session) error {
				oldCore = s.queryCore
				switch closeMode {
				case "close":

					return s.Close(ctx)
				case "hint":
					_, err := s.client.KeepAlive(ctx, &Ydb_Table.KeepAliveRequest{SessionId: s.ID()})

					return err
				default:
					s.SetStatus(table.SessionClosing)

					return nil
				}
			}))
			require.False(t, oldCore.IsAlive())
			require.NoError(t, a.sessionPool.With(context.Background(), func(_ context.Context, s *query.Session) error {
				require.NotSame(t, oldCore, s.Core)
				require.True(t, s.IsAlive())

				return nil
			}))
			require.EqualValues(t, 2, created.Load())
		})
	}
}

func TestSharedTableAdapterBadSession(t *testing.T) {
	a, created := newAdapterTestPool(t, &adapterTestConn{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attempts := 0
	var oldCore query.Core
	require.NoError(t, a.With(ctx, func(_ context.Context, s *Session) error {
		attempts++
		if attempts == 1 {
			oldCore = s.queryCore

			return xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
		}
		require.NotSame(t, oldCore, s.queryCore)

		return nil
	}))
	require.Equal(t, 2, attempts)
	require.EqualValues(t, 2, created.Load())
	require.False(t, oldCore.IsAlive())
}

type adapterTestCore struct {
	id     uint64
	status atomic.Uint32
	closes atomic.Uint32
}

func newAdapterTestCore(id uint64) *adapterTestCore {
	c := &adapterTestCore{id: id}
	c.SetStatus(query.StatusIdle)

	return c
}

func (c *adapterTestCore) ID() string {
	return fmt.Sprintf("session-%d", c.id)
}

func (c *adapterTestCore) NodeID() uint32 {
	return 42
}

func (c *adapterTestCore) Status() string {
	return query.Status(c.status.Load()).String()
}

func (c *adapterTestCore) SetStatus(s query.Status) {
	c.status.Store(uint32(s))
}

func (c *adapterTestCore) IsAlive() bool {
	return query.IsAlive(query.Status(c.status.Load()))
}

func (c *adapterTestCore) Close(context.Context) error {
	c.closes.Add(1)
	c.SetStatus(query.StatusClosed)

	return nil
}

type adapterTestConn struct{ invoke func(context.Context) }

func (c *adapterTestConn) Invoke(ctx context.Context, _ string, _, _ any, _ ...grpc.CallOption) error {
	if c.invoke != nil {
		c.invoke(ctx)
	}

	return nil
}

func (c *adapterTestConn) NewStream(
	context.Context, *grpc.StreamDesc, string, ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("unexpected stream")
}

func newAdapterTestPool(t *testing.T, cc grpc.ClientConnInterface) (*sessionPoolAdapter, *atomic.Uint64) {
	t.Helper()
	created := new(atomic.Uint64)
	p, err := pool.New(context.Background(),
		pool.WithLimit[*query.Session, query.Session](1),
		pool.WithCreateItemFunc(func(context.Context) (*query.Session, error) {
			return &query.Session{Core: newAdapterTestCore(created.Add(1))}, nil
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close(context.Background())) })

	return &sessionPoolAdapter{sessionPool: &query.SessionPool{Pool: p}, cc: cc, cfg: config.New()}, created
}
