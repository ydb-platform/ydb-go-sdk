//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type MockSpan struct{}

func (m MockSpan) ID() (id string, valid bool) {
	id = ""
	valid = true
	return id, valid
}

func (m MockSpan) TraceID() (id string, valid bool) {
	id = ""
	valid = true
	return id, valid
}

func (m MockSpan) Link(_ spans.Span, _ ...spans.KeyValue) {
}

func (m MockSpan) Log(_ string, _ ...spans.KeyValue) {
}

func (m MockSpan) Warn(_ error, _ ...spans.KeyValue) {
}

func (m MockSpan) Error(_ error, _ ...spans.KeyValue) {
}

func (m MockSpan) End(_ ...spans.KeyValue) {
}

type MockSpansAdapter struct{}

func (m MockSpansAdapter) Details() trace.Details {
	return trace.DetailsAll
}

func (m MockSpansAdapter) SpanFromContext(ctx context.Context) spans.Span {
	return &MockSpan{}
}

func (m MockSpansAdapter) Start(ctx context.Context, _ string, _ ...spans.KeyValue) (context.Context, spans.Span) {
	return ctx, &MockSpan{}
}

func TestDatabaseSQLBeginTxTracePanic(t *testing.T) {
	scope := newScope(t)

	ctx, cancel := context.WithCancel(scope.Ctx)
	defer cancel()

	nativeDriver := scope.Driver(
		ydb.WithSessionPoolSizeLimit(1),
		spans.WithTraces(MockSpansAdapter{}),
	)

	connector, err := ydb.Connector(nativeDriver)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := grpc.NewClient(scope.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		require.NoError(t, err)
	}(conn)
	tableClient := Ydb_Table_V1.NewTableServiceClient(conn)

	cc1, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, cc1)

	var ccID *string

	err = cc1.Raw(func(driverConn any) error {
		if ider, has := driverConn.(interface{ ID() string }); has {
			v := ider.ID()
			ccID = &v
		}

		return nil
	})
	require.NoError(t, err)

	require.NotNil(t, ccID)

	_, err = tableClient.DeleteSession(scope.Ctx, &Ydb_Table.DeleteSessionRequest{SessionId: *ccID})
	require.NoError(t, err)

	tx1, err := cc1.BeginTx(ctx, nil)
	require.Nil(t, tx1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Session not found")
}
