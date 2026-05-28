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
	done := tr.OnPoolGet(trace.QueryPoolGetStartInfo{
		Context: &ctx,
		Call:    testCall("OnPoolGet"),
	})
	require.NotNil(t, done)

	require.NotPanics(t, func() {
		done(trace.QueryPoolGetDoneInfo{
			Error:   errors.New("test error"),
			Session: (*panicSession)(nil),
		})
	})
}

func TestErrorPathDoesNotCallTableSessionMetadata(t *testing.T) {
	ctx := context.Background()
	tr := table(testAdapter{})
	done := tr.OnPoolGet(trace.TablePoolGetStartInfo{
		Context: &ctx,
		Call:    testCall("OnPoolGet"),
	})
	require.NotNil(t, done)

	require.NotPanics(t, func() {
		done(trace.TablePoolGetDoneInfo{
			Error:    errors.New("test error"),
			Attempts: 1,
			Session:  (*panicSession)(nil),
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

	sqlTrace := databaseSQL(testAdapter{})
	sqlDone := sqlTrace.OnConnBegin(trace.DatabaseSQLConnBeginStartInfo{
		Context: &ctx,
		Call:    testCall("OnConnBegin"),
	})
	require.NotNil(t, sqlDone)

	require.NotPanics(t, func() {
		sqlDone(trace.DatabaseSQLConnBeginDoneInfo{
			Error: errors.New("test error"),
			Tx:    (*panicTx)(nil),
		})
	})
}
