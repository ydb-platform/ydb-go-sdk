package spans

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type testAdapter struct{}

func (testAdapter) Details() trace.Details {
	return trace.DetailsAll
}

func (testAdapter) SpanFromContext(context.Context) Span {
	return testSpan{}
}

func (testAdapter) Start(ctx context.Context, _ string, _ ...KeyValue) (context.Context, Span) {
	return ctx, testSpan{}
}

type testSpan struct{}

func (testSpan) ID() (string, bool)       { return "", false }
func (testSpan) TraceID() (string, bool)  { return "", false }
func (testSpan) Link(Span, ...KeyValue)   {}
func (testSpan) Log(string, ...KeyValue)  {}
func (testSpan) Warn(error, ...KeyValue)  {}
func (testSpan) Error(error, ...KeyValue) {}
func (testSpan) End(...KeyValue)          {}

type testCall string

func (c testCall) String() string {
	return string(c)
}

type panicSession struct {
	id     string
	nodeID uint32
	status string
}

func (s *panicSession) ID() string {
	return s.id
}

func (s *panicSession) NodeID() uint32 {
	return s.nodeID
}

func (s *panicSession) Status() string {
	return s.status
}

type panicTx struct {
	id string
}

func (tx *panicTx) ID() string {
	return tx.id
}

func TestErrorPathDoesNotCallSessionMetadata(t *testing.T) {
	ctx := context.Background()
	tr := query(testAdapter{})
	poolGetDone := tr.OnPoolGet(trace.QueryPoolGetStartInfo{
		Context: &ctx,
		Call:    testCall("OnPoolGet"),
	})
	require.NotNil(t, poolGetDone)

	require.NotPanics(t, func() {
		poolGetDone(trace.QueryPoolGetDoneInfo{
			Error:   errors.New("test error"),
			Session: (*panicSession)(nil),
		})
	})

	sessionCreateDone := tr.OnSessionCreate(trace.QuerySessionCreateStartInfo{
		Context: &ctx,
		Call:    testCall("OnSessionCreate"),
	})
	require.NotNil(t, sessionCreateDone)

	require.NotPanics(t, func() {
		sessionCreateDone(trace.QuerySessionCreateDoneInfo{
			Error:   errors.New("test error"),
			Session: (*panicSession)(nil),
		})
	})
}

func TestErrorPathDoesNotCallTableSessionMetadata(t *testing.T) {
	ctx := context.Background()
	tr := table(testAdapter{})
	poolGetDone := tr.OnPoolGet(trace.TablePoolGetStartInfo{
		Context: &ctx,
		Call:    testCall("OnPoolGet"),
	})
	require.NotNil(t, poolGetDone)

	require.NotPanics(t, func() {
		poolGetDone(trace.TablePoolGetDoneInfo{
			Error:    errors.New("test error"),
			Attempts: 1,
			Session:  (*panicSession)(nil),
		})
	})

	sessionNewDone := tr.OnSessionNew(trace.TableSessionNewStartInfo{
		Context: &ctx,
		Call:    testCall("OnSessionNew"),
	})
	require.NotNil(t, sessionNewDone)

	require.NotPanics(t, func() {
		sessionNewDone(trace.TableSessionNewDoneInfo{
			Error:   errors.New("test error"),
			Session: (*panicSession)(nil),
		})
	})
}

func TestErrorPathDoesNotCallTxMetadata(t *testing.T) {
	ctx := context.Background()
	q := query(testAdapter{})
	qDone := q.OnSessionBegin(trace.QuerySessionBeginStartInfo{
		Context: &ctx,
		Call:    testCall("OnSessionBegin"),
	})
	require.NotNil(t, qDone)

	require.NotPanics(t, func() {
		qDone(trace.QuerySessionBeginDoneInfo{
			Error: errors.New("test error"),
			Tx:    (*panicTx)(nil),
		})
	})

	tableTrace := table(testAdapter{})
	tableTxBeginDone := tableTrace.OnTxBegin(trace.TableTxBeginStartInfo{
		Context: &ctx,
		Call:    testCall("OnTxBegin"),
	})
	require.NotNil(t, tableTxBeginDone)

	require.NotPanics(t, func() {
		tableTxBeginDone(trace.TableTxBeginDoneInfo{
			Error: errors.New("test error"),
			Tx:    (*panicTx)(nil),
		})
	})

	sqlTrace := databaseSQL(testAdapter{})
	connBeginDone := sqlTrace.OnConnBegin(trace.DatabaseSQLConnBeginStartInfo{
		Context: &ctx,
		Call:    testCall("OnConnBegin"),
	})
	require.NotNil(t, connBeginDone)

	require.NotPanics(t, func() {
		connBeginDone(trace.DatabaseSQLConnBeginDoneInfo{
			Error: errors.New("test error"),
			Tx:    (*panicTx)(nil),
		})
	})

	connBeginTxDone := sqlTrace.OnConnBeginTx(trace.DatabaseSQLConnBeginTxStartInfo{
		Context: &ctx,
		Call:    testCall("OnConnBeginTx"),
	})
	require.NotNil(t, connBeginTxDone)

	require.NotPanics(t, func() {
		connBeginTxDone(trace.DatabaseSQLConnBeginTxDoneInfo{
			Error: errors.New("test error"),
			Tx:    (*panicTx)(nil),
		})
	})
}
