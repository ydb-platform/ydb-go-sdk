package spans

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type fakeSession struct {
	id     string
	status string
	nodeID uint32
}

func (s *fakeSession) ID() string     { return s.id }
func (s *fakeSession) NodeID() uint32 { return s.nodeID }
func (s *fakeSession) Status() string { return s.status }

type fakeTx struct{ id string }

func (t *fakeTx) ID() string { return t.id }

// trace.txInfo only requires ID() string.
var _ interface{ ID() string } = (*fakeTx)(nil)

func TestQuerySpanNamesAreOTelCompliant(t *testing.T) {
	adapter := &recordingAdapter{}
	q := query(adapter)

	ctx := context.Background()
	call := stack.FunctionID("test.caller")
	session := &fakeSession{id: "session-1", status: "ready", nodeID: 42}
	tx := &fakeTx{id: "tx-1"}

	t.Run("ydb.CreateSession", func(t *testing.T) {
		c := ctx
		done := q.OnSessionCreate(trace.QuerySessionCreateStartInfo{
			Context: &c,
			Call:    call,
		})
		require.NotNil(t, done)
		done(trace.QuerySessionCreateDoneInfo{Session: session})

		spans := adapter.byName(SpanNameCreateSession)
		require.Len(t, spans, 1)
		require.True(t, spans[0].ended)
		require.Equal(t, int64(42), spans[0].attr(AttrYDBNodeID))
	})

	t.Run("ydb.ExecuteQuery on Query", func(t *testing.T) {
		c := ctx
		done := q.OnQuery(trace.QueryQueryStartInfo{
			Context: &c,
			Call:    call,
			Query:   "  SELECT 1  ",
		})
		require.NotNil(t, done)
		done(trace.QueryQueryDoneInfo{})

		spans := adapter.byName(SpanNameExecuteQuery)
		require.NotEmpty(t, spans)
	})

	t.Run("ydb.ExecuteQuery on SessionQuery sets node id", func(t *testing.T) {
		c := ctx
		done := q.OnSessionQuery(trace.QuerySessionQueryStartInfo{
			Context: &c,
			Call:    call,
			Session: session,
			Query:   "SELECT 2",
		})
		require.NotNil(t, done)
		done(trace.QuerySessionQueryDoneInfo{})

		spans := adapter.byName(SpanNameExecuteQuery)
		s := spans[len(spans)-1]
		require.Equal(t, int64(42), s.attr(AttrYDBNodeID))
	})

	t.Run("ydb.BeginTransaction emitted for actual gRPC begin", func(t *testing.T) {
		c := ctx
		done := q.OnSessionBeginTransaction(trace.QuerySessionBeginTransactionStartInfo{
			Context: &c,
			Call:    call,
			Session: session,
		})
		require.NotNil(t, done)
		done(trace.QuerySessionBeginTransactionDoneInfo{TxID: tx.ID()})

		spans := adapter.byName(SpanNameBeginTransaction)
		require.Len(t, spans, 1)
		require.True(t, spans[0].ended)
		require.Nil(t, spans[0].err)
		require.Equal(t, int64(42), spans[0].attr(AttrYDBNodeID))
	})

	t.Run("ydb.Commit", func(t *testing.T) {
		c := ctx
		done := q.OnTxCommit(trace.QueryTxCommitStartInfo{
			Context: &c,
			Call:    call,
			Session: session,
			Tx:      tx,
		})
		require.NotNil(t, done)
		done(trace.QueryTxCommitDoneInfo{})

		spans := adapter.byName(SpanNameCommit)
		require.Len(t, spans, 1)
		require.Equal(t, int64(42), spans[0].attr(AttrYDBNodeID))
	})

	t.Run("ydb.Rollback", func(t *testing.T) {
		c := ctx
		done := q.OnTxRollback(trace.QueryTxRollbackStartInfo{
			Context: &c,
			Call:    call,
			Session: session,
			Tx:      tx,
		})
		require.NotNil(t, done)
		done(trace.QueryTxRollbackDoneInfo{})

		spans := adapter.byName(SpanNameRollback)
		require.Len(t, spans, 1)
		require.Equal(t, int64(42), spans[0].attr(AttrYDBNodeID))
	})

	t.Run("ydb.ExecuteQuery on TxQueryResultSet sets node id", func(t *testing.T) {
		c := ctx
		done := q.OnTxQueryResultSet(trace.QueryTxQueryResultSetStartInfo{
			Context: &c,
			Call:    call,
			Session: session,
			Tx:      tx,
			Query:   "SELECT 3",
		})
		require.NotNil(t, done)
		done(trace.QueryTxQueryResultSetDoneInfo{})

		spans := adapter.byName(SpanNameExecuteQuery)
		s := spans[len(spans)-1]
		require.Equal(t, int64(42), s.attr(AttrYDBNodeID))
	})

	t.Run("ydb.ExecuteQuery on TxQueryRow sets node id", func(t *testing.T) {
		c := ctx
		done := q.OnTxQueryRow(trace.QueryTxQueryRowStartInfo{
			Context: &c,
			Call:    call,
			Session: session,
			Tx:      tx,
			Query:   "SELECT 4",
		})
		require.NotNil(t, done)
		done(trace.QueryTxQueryRowDoneInfo{})

		spans := adapter.byName(SpanNameExecuteQuery)
		s := spans[len(spans)-1]
		require.Equal(t, int64(42), s.attr(AttrYDBNodeID))
	})
}

func TestQueryNoisySpansAreSuppressed(t *testing.T) {
	adapter := &recordingAdapter{}
	q := query(adapter)

	ctx := context.Background()
	call := stack.FunctionID("test.caller")

	// OnDo / OnDoTx / OnSessionBegin / OnPoolWith / OnPoolTry / OnPoolPut
	// must NOT register handlers — they are intentionally suppressed so the
	// span tree only shows ydb.* user-facing names.
	require.Nil(t, q.OnDo)
	require.Nil(t, q.OnDoTx)
	require.Nil(t, q.OnSessionBegin)
	require.Nil(t, q.OnSessionDelete)
	require.Nil(t, q.OnPoolWith)
	require.Nil(t, q.OnPoolTry)
	require.Nil(t, q.OnPoolPut)

	// OnSessionBeginTransaction IS wired: it fires only for an actual
	// BeginTransaction gRPC call and produces a single ydb.BeginTransaction
	// CLIENT span.
	require.NotNil(t, q.OnSessionBeginTransaction)

	// OnPoolGet is wired and produces a single ydb.GetSession span.
	c := ctx
	done := q.OnPoolGet(trace.QueryPoolGetStartInfo{Context: &c, Call: call})
	require.NotNil(t, done)
	done(trace.QueryPoolGetDoneInfo{})
	require.Len(t, adapter.byName(SpanNameGetSession), 1)
}

func TestQuerySpanFailureSetsExceptionAttrs(t *testing.T) {
	adapter := &recordingAdapter{}
	q := query(adapter)
	ctx := context.Background()
	call := stack.FunctionID("test.caller")

	t.Run("ydb error sets db.response.status_code and error.type=ydb_error", func(t *testing.T) {
		c := ctx
		done := q.OnTxCommit(trace.QueryTxCommitStartInfo{
			Context: &c,
			Call:    call,
			Session: &fakeSession{id: "s"},
			Tx:      &fakeTx{id: "t"},
		})
		require.NotNil(t, done)
		ydbErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
		done(trace.QueryTxCommitDoneInfo{Error: ydbErr})

		spans := adapter.byName(SpanNameCommit)
		require.Len(t, spans, 1)
		s := spans[0]
		require.NotNil(t, s.err)
		require.Equal(t, ErrorTypeYDB, s.attr(AttrErrorType))
		require.Equal(t, int(Ydb.StatusIds_BAD_SESSION), s.attr(AttrDBResponseStatusCode))
	})

	t.Run("ydb.BeginTransaction failure sets error.type", func(t *testing.T) {
		c := ctx
		done := q.OnSessionBeginTransaction(trace.QuerySessionBeginTransactionStartInfo{
			Context: &c,
			Call:    call,
			Session: &fakeSession{id: "s"},
		})
		require.NotNil(t, done)
		ydbErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
		done(trace.QuerySessionBeginTransactionDoneInfo{Error: ydbErr})

		spans := adapter.byName(SpanNameBeginTransaction)
		require.Len(t, spans, 1)
		s := spans[0]
		require.NotNil(t, s.err)
		require.Equal(t, ErrorTypeYDB, s.attr(AttrErrorType))
		require.Equal(t, int(Ydb.StatusIds_BAD_SESSION), s.attr(AttrDBResponseStatusCode))
	})

	t.Run("plain error sets error.type to dynamic Go type", func(t *testing.T) {
		c := ctx
		done := q.OnTxRollback(trace.QueryTxRollbackStartInfo{
			Context: &c,
			Call:    call,
			Session: &fakeSession{id: "s"},
			Tx:      &fakeTx{id: "t"},
		})
		require.NotNil(t, done)
		err := errors.New("boom")
		done(trace.QueryTxRollbackDoneInfo{Error: err})

		spans := adapter.byName(SpanNameRollback)
		require.NotEmpty(t, spans)
		s := spans[len(spans)-1]
		require.NotNil(t, s.err)
		require.Equal(t, "*errors.errorString", s.attr(AttrErrorType))
	})
}
