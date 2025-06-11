package table

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
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
