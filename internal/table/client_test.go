package table

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	internalconn "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestRaceWgClosed(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			t.Fatal(e)
		}
	}()

	var (
		limit   = 100
		start   = time.Now()
		counter int
	)

	xtest.TestManyTimes(t, func(t testing.TB) {
		counter++
		defer func() {
			if counter%1000 == 0 {
				t.Logf("%0.1fs: %d times test passed", time.Since(start).Seconds(), counter)
			}
		}()
		ctx, cancel := xcontext.WithTimeout(context.Background(),
			//nolint:gosec
			time.Duration(rand.Int31n(int32(100*time.Millisecond))),
		)
		defer cancel()

		wg := sync.WaitGroup{}
		p := New(ctx,
			testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
				testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
					return &Ydb_Table.CreateSessionResult{
						SessionId: testutil.SessionID(),
					}, nil
				},
			})),
			config.New(config.WithSizeLimit(limit)),
		)
		for j := 0; j < limit*10; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					err := p.Do(ctx,
						func(ctx context.Context, s table.Session) error {
							return nil
						},
					)
					if err != nil && xerrors.Is(err, errClosedClient) {
						return
					}
				}
			}()
		}
		_ = p.Close(context.Background())
		wg.Wait()
	}, xtest.StopAfter(27*time.Second))
}

func TestChunkBulkUpsertRequest(t *testing.T) {
	t.Run("empty request", func(t *testing.T) {
		input := newTestBulkRequest(t, 0)
		got, err := chunkBulkUpsertRequest(nil, input, 100)
		require.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, input, got[0])
	})

	t.Run("one chunk greater than maxSize", func(t *testing.T) {
		input := newTestBulkRequest(t, 1)
		_, err := chunkBulkUpsertRequest(nil, input, 10)
		assert.Error(t, err)
	})

	t.Run("one request", func(t *testing.T) {
		input := newTestBulkRequest(t, 50)
		got, err := chunkBulkUpsertRequest(nil, input, 100)
		require.NoError(t, err)
		assert.Len(t, got, 2)
		assert.Less(t, proto.Size(got[0]), 100)
		assert.Less(t, proto.Size(got[1]), 100)
	})

	t.Run("zero max size", func(t *testing.T) {
		input := newTestBulkRequest(t, 50)
		_, err := chunkBulkUpsertRequest(nil, input, 0)
		assert.Error(t, err)
	})
}

func newTestBulkRequest(t *testing.T, itemsLen int) *Ydb_Table.BulkUpsertRequest {
	t.Helper()

	rows := make([]types.Value, itemsLen)

	for i := range itemsLen {
		rows[i] = types.StructValue()
	}

	req, err := table.BulkUpsertDataRows(
		types.ListValue(rows...),
	).ToYDB("testTable")
	require.NoError(t, err)

	return req
}

var okHandler = func(interface{}) (proto.Message, error) {
	return &emptypb.Empty{}, nil
}

var simpleCluster = testutil.NewBalancer(
	testutil.WithInvokeHandlers(
		testutil.InvokeHandlers{
			testutil.TableExecuteDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.ExecuteQueryResult{
					TxMeta: &Ydb_Table.TransactionMeta{
						Id: "",
					},
				}, nil
			},
			testutil.TableBeginTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.BeginTransactionResult{
					TxMeta: &Ydb_Table.TransactionMeta{
						Id: "",
					},
				}, nil
			},
			testutil.TableExplainDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.ExecuteQueryResult{}, nil
			},
			testutil.TablePrepareDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.PrepareQueryResult{}, nil
			},
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			},
			testutil.TableDeleteSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.DeleteSessionResponse{}, nil
			},
			testutil.TableCommitTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CommitTransactionResponse{}, nil
			},
			testutil.TableRollbackTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.RollbackTransactionResponse{}, nil
			},
			testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.KeepAliveResult{}, nil
			},
		},
	),
)

func simpleSession(t testing.TB) *Session {
	s, err := newTableSession(context.Background(), simpleCluster, config.New())
	if err != nil {
		t.Fatalf("newTableSession unexpected error: %v", err)
	}

	return s
}

type StubBuilder struct {
	OnCreateSession func(ctx context.Context) (*Session, error)

	cc    grpc.ClientConnInterface
	Limit int
	T     testing.TB

	mu     xsync.Mutex
	actual int
}

func (s *StubBuilder) createSession(ctx context.Context) (session *Session, err error) {
	defer s.mu.WithLock(func() {
		if session != nil {
			s.actual++
		}
	})

	s.mu.WithLock(func() {
		if s.Limit > 0 && s.actual == s.Limit {
			err = fmt.Errorf("stub session: limit overflow")
		}
	})
	if err != nil {
		return nil, err
	}

	if f := s.OnCreateSession; f != nil {
		return f(ctx)
	}

	return newTableSession(ctx, s.cc, config.New())
}

func TestCreateExplicitSession(t *testing.T) {
	ctx := xtest.Context(t)

	t.Run("HappyPath", func(t *testing.T) {
		cc := &clientStateSettingConn{}

		c := New(ctx, cc, config.New())
		defer func() { _ = c.Close(ctx) }()

		err := c.Do(ctx, func(ctx context.Context, s table.Session) error {
			return nil
		})

		require.NoError(t, err)
		require.NotEqual(t, internalconn.Banned, cc.state())
	})

	t.Run("BansConnectionOnOverloadedCreateSession", func(t *testing.T) {
		cc := &clientStateSettingConn{
			invokeErr: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
		}

		c := New(ctx, cc, config.New())
		defer func() { _ = c.Close(ctx) }()

		ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		_ = c.Do(ctxTimeout, func(ctx context.Context, s table.Session) error {
			return nil
		})

		require.Equal(t, internalconn.Banned, cc.state())
	})

	t.Run("DoesNotBanConnectionOnNonOverloadedError", func(t *testing.T) {
		cc := &clientStateSettingConn{
			invokeErr: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		}

		c := New(ctx, cc, config.New())
		defer func() { _ = c.Close(ctx) }()

		ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		_ = c.Do(ctxTimeout, func(ctx context.Context, s table.Session) error {
			return nil
		})

		require.NotEqual(t, internalconn.Banned, cc.state())
	})
}

// clientStateSettingConn is a test helper that implements grpc.ClientConnInterface and conn.StateSetter.
// When Invoke is called with invokeErr != nil, it returns the error.
// When invokeErr is nil, it populates reply with a valid session response for happy path testing.
// SetState calls are recorded to verify pessimization behavior.
type clientStateSettingConn struct {
	mu        sync.Mutex
	lastState internalconn.State
	invokeErr error
}

func (c *clientStateSettingConn) SetState(_ context.Context, s internalconn.State) internalconn.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastState = s

	return s
}

func (c *clientStateSettingConn) state() internalconn.State {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastState
}

func (c *clientStateSettingConn) Invoke(
	_ context.Context, _ string, _ any, reply any, _ ...grpc.CallOption,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.invokeErr != nil {
		return c.invokeErr
	}

	switch r := reply.(type) {
	case *Ydb_Table.CreateSessionResponse:
		sessionResult, _ := anypb.New(&Ydb_Table.CreateSessionResult{SessionId: "test-session"})
		r.Operation = &Ydb_Operations.Operation{
			Status: Ydb.StatusIds_SUCCESS,
			Ready:  true,
			Result: sessionResult,
		}
	case *Ydb_Table.DeleteSessionResponse:
		r.Operation = &Ydb_Operations.Operation{
			Status: Ydb.StatusIds_SUCCESS,
			Ready:  true,
		}
	}

	return nil
}

func (c *clientStateSettingConn) NewStream(
	_ context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errClientStateSettingConnNotImplemented
}

var errClientStateSettingConnNotImplemented = errors.New("not implemented")
