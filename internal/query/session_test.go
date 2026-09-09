package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TestSessionBeginLazyTxDeadSession ensures that Begin returns BAD_SESSION
// for a lazy transaction when the session is no longer alive. This prevents
// the dead session from being silently reused inside a new transaction.
func TestSessionBeginLazyTxDeadSession(t *testing.T) {
	ctx := t.Context()

	// Create a session whose underlying Core reports IsAlive() == false
	// (simulates a session that was previously invalidated by BAD_SESSION).
	deadCore := &sessionControllerMock{
		id:     "dead-session",
		status: StatusError,
	}
	s := &Session{
		Core:   deadCore,
		trace:  &trace.Query{},
		lazyTx: true, // lazy-tx mode
	}

	// Begin should refuse to create a lazy transaction for a dead session.
	lazyCtx := baseTx.WithLazyTx(ctx, true)
	tx, err := s.Begin(lazyCtx, query.TxSettings(query.WithSerializableReadWrite()))
	require.Error(t, err)
	require.Nil(t, tx)
	require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION))
}

func TestSessionClosedOnQueryError(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		reason string
	}{
		{
			name:   "ClientQueryTimeout",
			err:    context.DeadlineExceeded,
			reason: "client_timeout",
		},
		{
			name:   "QueryStreamCancelledByClient",
			err:    context.Canceled,
			reason: "client_cancelled",
		},
		{
			name:   "TransportError",
			err:    grpcStatus.Error(grpcCodes.Unavailable, "query failed"),
			reason: "transport_error",
		},
		{
			name:   "BadSession",
			err:    xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			reason: "bad_session",
		},
		{
			name:   "SessionExpired",
			err:    xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED)),
			reason: "bad_session",
		},
		{
			name:   "SessionBusy",
			err:    xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY)),
			reason: "session_busy",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			gomock.InOrder(
				stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil),
				stream.EXPECT().Recv().Return(nil, test.err),
			)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

			var events []trace.QuerySessionClosedInfo
			queryTrace := &trace.Query{
				OnSessionClosed: func(info trace.QuerySessionClosedInfo) {
					events = append(events, info)
				},
			}
			core := &sessionCore{
				Client:   client,
				Trace:    queryTrace,
				id:       "123",
				poolName: "/local",
			}
			core.status.Store(uint32(StatusIdle))
			s := &Session{
				Core:   core,
				client: client,
				trace:  queryTrace,
			}

			r, err := s.execute(t.Context(), "SELECT 1", options.ExecuteSettings(), options.ResultSetsTypeOrdered)
			require.NoError(t, err)
			_, err = r.nextPart(t.Context())
			require.Error(t, err)
			poolTrace(queryTrace, "/local").OnCloseItem(s, "pool_graceful_shutdown")

			require.Equal(t, []trace.QuerySessionClosedInfo{{
				PoolName: "/local",
				Reason:   test.reason,
			}}, events)
			require.False(t, s.IsAlive())
		})
	}
}

func TestSessionClosedIgnoresUnrelatedYDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	gomock.InOrder(
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil),
		stream.EXPECT().Recv().Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE))),
	)
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

	var events []trace.QuerySessionClosedInfo
	queryTrace := &trace.Query{
		OnSessionClosed: func(info trace.QuerySessionClosedInfo) {
			events = append(events, info)
		},
	}
	core := &sessionCore{
		Client:   client,
		Trace:    queryTrace,
		id:       "123",
		poolName: "/local",
	}
	core.status.Store(uint32(StatusIdle))
	s := &Session{
		Core:   core,
		client: client,
		trace:  queryTrace,
	}

	r, err := s.execute(t.Context(), "SELECT 1", options.ExecuteSettings(), options.ResultSetsTypeOrdered)
	require.NoError(t, err)
	_, err = r.nextPart(t.Context())
	require.Error(t, err)
	require.Empty(t, events)
	require.True(t, s.IsAlive())
}

func TestSessionClosedIgnoresEOFAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	gomock.InOrder(
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil),
		stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			cancel()

			return nil, io.EOF
		}),
	)
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

	var events []trace.QuerySessionClosedInfo
	queryTrace := &trace.Query{
		OnSessionClosed: func(info trace.QuerySessionClosedInfo) {
			events = append(events, info)
		},
	}
	core := &sessionCore{
		Client:   client,
		Trace:    queryTrace,
		id:       "123",
		poolName: "/local",
	}
	core.status.Store(uint32(StatusIdle))
	s := &Session{
		Core:   core,
		client: client,
		trace:  queryTrace,
	}

	r, err := s.execute(t.Context(), "SELECT 1", options.ExecuteSettings(), options.ResultSetsTypeOrdered)
	require.NoError(t, err)
	_, err = r.nextPart(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, events)
	require.True(t, s.IsAlive())
}

func TestSessionClosedOnInitialQueryTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 0)
	defer cancel()

	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(nil, context.Canceled).AnyTimes()
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

	var events []trace.QuerySessionClosedInfo
	queryTrace := &trace.Query{
		OnSessionClosed: func(info trace.QuerySessionClosedInfo) {
			events = append(events, info)
		},
	}
	core := &sessionCore{
		Client:   client,
		Trace:    queryTrace,
		id:       "123",
		poolName: "/local",
	}
	core.status.Store(uint32(StatusIdle))
	s := &Session{
		Core:   core,
		client: client,
		trace:  queryTrace,
	}

	_, err := s.execute(ctx, "SELECT 1", options.ExecuteSettings(), options.ResultSetsTypeOrdered)
	require.Error(t, err)
	require.Equal(t, []trace.QuerySessionClosedInfo{{
		PoolName: "/local",
		Reason:   "client_timeout",
	}}, events)
}

func TestSessionClosedOnCanceledResultOperations(t *testing.T) {
	tests := []struct {
		name string
		run  func(activeCtx, cancelCtx context.Context, cancel context.CancelFunc, r query.Result) error
	}{
		{
			name: "NextResultSet",
			run: func(_ context.Context, cancelCtx context.Context, cancel context.CancelFunc, r query.Result) error {
				cancel()
				_, err := r.NextResultSet(cancelCtx)

				return err
			},
		},
		{
			name: "NextRowBetweenParts",
			run: func(activeCtx, cancelCtx context.Context, cancel context.CancelFunc, r query.Result) error {
				rs, err := r.NextResultSet(cancelCtx)
				if err != nil {
					return err
				}
				cancel()
				_, err = rs.NextRow(activeCtx)

				return err
			},
		},
		{
			name: "NextRow",
			run: func(activeCtx, cancelCtx context.Context, cancel context.CancelFunc, r query.Result) error {
				rs, err := r.NextResultSet(activeCtx)
				if err != nil {
					return err
				}
				cancel()
				_, err = rs.NextRow(cancelCtx)

				return err
			},
		},
		{
			name: "Close",
			run: func(_ context.Context, cancelCtx context.Context, cancel context.CancelFunc, r query.Result) error {
				cancel()

				return r.Close(cancelCtx)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet:      &Ydb.ResultSet{},
			}, nil)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
			s, events := newPooledSessionClosedRecorder(client)

			activeCtx := t.Context()
			cancelCtx, cancel := context.WithCancel(activeCtx)
			r, err := s.Query(activeCtx, "SELECT 1")
			require.NoError(t, err)
			t.Cleanup(func() {
				cancel()
				_ = r.Close(cancelCtx)
			})

			err = test.run(activeCtx, cancelCtx, cancel, r)

			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, []trace.QuerySessionClosedInfo{{
				PoolName: "/local",
				Reason:   "client_cancelled",
			}}, *events)
			require.False(t, s.IsAlive())
		})
	}
}

func TestSessionClosedOnBeginError(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil,
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
	)
	s, events := newPooledSessionClosedRecorder(client)

	_, err := s.Begin(t.Context(), query.TxSettings())

	require.Error(t, err)
	require.Equal(t, []trace.QuerySessionClosedInfo{{
		PoolName: "/local",
		Reason:   "bad_session",
	}}, *events)
	require.False(t, s.IsAlive())
}

func TestSessionClosedOnUnLazyError(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil,
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
	)
	s, events := newPooledSessionClosedRecorder(client)
	tx := &Transaction{
		LazyID:     baseTx.LazyID{},
		s:          s,
		txSettings: query.TxSettings(),
	}

	err := tx.UnLazy(t.Context())

	require.Error(t, err)
	require.Equal(t, []trace.QuerySessionClosedInfo{{
		PoolName: "/local",
		Reason:   "bad_session",
	}}, *events)
	require.False(t, s.IsAlive())
}

func TestSessionClosedOnTransactionRPCError(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		setup  func(*MockQueryServiceClient)
		run    func(context.Context, *Transaction) error
	}{
		{
			name:   "CommitTx",
			reason: "transport_error",
			setup: func(client *MockQueryServiceClient) {
				client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(nil,
					grpcStatus.Error(grpcCodes.Unavailable, "commit failed"),
				)
			},
			run: func(ctx context.Context, tx *Transaction) error {
				return tx.CommitTx(ctx)
			},
		},
		{
			name:   "Rollback",
			reason: "session_busy",
			setup: func(client *MockQueryServiceClient) {
				client.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(nil,
					xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY)),
				)
			},
			run: func(ctx context.Context, tx *Transaction) error {
				return tx.Rollback(ctx)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			test.setup(client)
			s, events := newPooledSessionClosedRecorder(client)
			tx := &Transaction{LazyID: baseTx.ID("456"), s: s}

			err := test.run(t.Context(), tx)

			require.Error(t, err)
			require.Equal(t, []trace.QuerySessionClosedInfo{{
				PoolName: "/local",
				Reason:   test.reason,
			}}, *events)
			require.False(t, s.IsAlive())
		})
	}
}

func TestSessionClosedOnInitialArrowError(t *testing.T) {
	t.Run("ClientCancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx, cancel := context.WithCancel(t.Context())
		stream := newExecuteQueryStreamMock(ctrl)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, *Ydb_Query.ExecuteQueryRequest, ...grpc.CallOption) (
				Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
			) {
				cancel()

				return stream, nil
			},
		)
		s, events := newPooledSessionClosedRecorder(client)

		r, err := s.QueryArrow(ctx, "SELECT 1")

		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, r)
		require.Equal(t, []trace.QuerySessionClosedInfo{{
			PoolName: "/local",
			Reason:   "client_cancelled",
		}}, *events)
		require.False(t, s.IsAlive())
	})

	t.Run("TransportError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(nil,
			grpcStatus.Error(grpcCodes.Unavailable, "execute failed"),
		)
		s, events := newPooledSessionClosedRecorder(client)

		r, err := s.QueryArrow(t.Context(), "SELECT 1")

		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
		require.Nil(t, r)
		require.Equal(t, []trace.QuerySessionClosedInfo{{
			PoolName: "/local",
			Reason:   "transport_error",
		}}, *events)
		require.False(t, s.IsAlive())
	})
}

func TestSessionClosedOnArrowStreamError(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(nil, grpcStatus.Error(grpcCodes.Unavailable, "stream failed"))
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
	s, events := newPooledSessionClosedRecorder(client)

	r, err := s.QueryArrow(t.Context(), "SELECT 1")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = r.Close(t.Context())
	})
	var streamErr error
	for _, err = range r.Parts(t.Context()) {
		streamErr = err
	}
	require.NoError(t, r.Close(t.Context()))

	require.Error(t, streamErr)
	require.True(t, xerrors.IsTransportError(streamErr, grpcCodes.Unavailable))
	require.Equal(t, []trace.QuerySessionClosedInfo{{
		PoolName: "/local",
		Reason:   "transport_error",
	}}, *events)
	require.False(t, s.IsAlive())
}

func TestSessionClosedOnUnfinishedArrowClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
	s, events := newPooledSessionClosedRecorder(client)

	r, err := s.QueryArrow(t.Context(), "SELECT 1")
	require.NoError(t, err)

	err = r.Close(t.Context())

	require.NoError(t, err)
	require.Equal(t, []trace.QuerySessionClosedInfo{{
		PoolName: "/local",
		Reason:   "client_cancelled",
	}}, *events)
	require.False(t, s.IsAlive())
}

func TestSessionClosedOnCanceledArrowRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	var executeCtx context.Context
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
		cancel()
		<-executeCtx.Done()

		return nil, executeCtx.Err()
	})
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
			Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
		) {
			executeCtx = ctx

			return stream, nil
		},
	)
	s, events := newPooledSessionClosedRecorder(client)

	r, err := s.QueryArrow(t.Context(), "SELECT 1")
	require.NoError(t, err)
	var streamErr error
	for _, err = range r.Parts(ctx) {
		streamErr = err
	}
	require.NoError(t, r.Close(t.Context()))

	require.ErrorIs(t, streamErr, context.Canceled)
	require.Equal(t, []trace.QuerySessionClosedInfo{{
		PoolName: "/local",
		Reason:   "client_cancelled",
	}}, *events)
	require.False(t, s.IsAlive())
}

func TestSessionClosedIgnoresArrowEOFAfterContextCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(t.Context())
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
		cancel()

		return nil, io.EOF
	})
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
	s, events := newPooledSessionClosedRecorder(client)

	r, err := s.QueryArrow(t.Context(), "SELECT 1")
	require.NoError(t, err)
	for _, partErr := range r.Parts(ctx) {
		require.NoError(t, partErr)
	}
	require.NoError(t, r.Close(t.Context()))

	require.Empty(t, *events)
	require.True(t, s.IsAlive())
}

func TestCreateSession(t *testing.T) {
	trace := &trace.Query{
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
			return func(info trace.QuerySessionCreateDoneInfo) {
				if info.Session != nil && info.Error != nil {
					panic("only one result from tuple may be not nil")
				}
			}
		},
	}
	t.Run("HappyWay", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		stubAttachStreamContext(attachStream)
		attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)
		attachStream.EXPECT().Recv().Return(nil, errSessionClosed).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)
		require.NotPanics(t, func() {
			s, err := createSession(ctx, client, WithTrace(trace))
			require.NoError(t, err)
			require.NotNil(t, s)
			require.Equal(t, "123", s.ID())
		})
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("OnCreateSession", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil,
				xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test")),
			)
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
		t.Run("OnAttachStream", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "123",
			}, nil)
			client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
				SessionId: "123",
			}).Return(nil, xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test")))
			client.EXPECT().DeleteSession(gomock.Any(), &Ydb_Query.DeleteSessionRequest{
				SessionId: "123",
			}).Return(&Ydb_Query.DeleteSessionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("OnCreateSession", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil,
				xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
			)
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
		t.Run("OnAttachStream", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "123",
			}, nil)
			client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
				SessionId: "123",
			}).Return(nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)))
			client.EXPECT().DeleteSession(gomock.Any(), &Ydb_Query.DeleteSessionRequest{
				SessionId: "123",
			}).Return(&Ydb_Query.DeleteSessionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			require.NotPanics(t, func() {
				s, err := createSession(ctx, client, WithTrace(trace))
				require.Error(t, err)
				require.Nil(t, s)
			})
		})
	})
}

func newPooledSessionClosedRecorder(
	client *MockQueryServiceClient,
) (*Session, *[]trace.QuerySessionClosedInfo) {
	var events []trace.QuerySessionClosedInfo
	queryTrace := &trace.Query{
		OnSessionClosed: func(info trace.QuerySessionClosedInfo) {
			events = append(events, info)
		},
	}
	core := &sessionCore{
		Client:   client,
		Trace:    queryTrace,
		id:       "123",
		poolName: "/local",
	}
	core.status.Store(uint32(StatusIdle))

	return &Session{
		Core:   core,
		client: client,
		trace:  queryTrace,
	}, &events
}
