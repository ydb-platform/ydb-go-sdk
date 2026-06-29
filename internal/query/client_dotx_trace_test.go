package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestClient_DoTx_CallLevelTrace(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)

	client := NewMockQueryServiceClient(ctrl)
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
		client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)

		return &Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "tx-1",
			},
		}, nil
	})
	stream.EXPECT().Recv().Return(nil, io.EOF)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

	var onDoTxCalled, onTxExecCalled int
	callTrace := &trace.Query{
		OnDoTx: func(info trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			onDoTxCalled++

			return func(trace.QueryDoTxDoneInfo) {}
		},
		OnTxExec: func(info trace.QueryTxExecStartInfo) func(trace.QueryTxExecDoneInfo) {
			onTxExecCalled++

			return func(trace.QueryTxExecDoneInfo) {}
		},
	}

	c := testClient(t, client)
	c.explicitSessionPool = &mockSessionPool{
		withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
			return f(ctx, newTestSessionWithClient("s-1", client, true))
		},
	}

	err := c.DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		return tx.Exec(ctx, "SELECT 1")
	}, options.WithTrace(callTrace))
	require.NoError(t, err)
	require.Equal(t, 1, onDoTxCalled, "call-level OnDoTx must be invoked")
	require.Equal(t, 1, onTxExecCalled, "call-level OnTxExec must be invoked inside DoTx")
}

func TestClient_Do_CallLevelTrace(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)

	client := NewMockQueryServiceClient(ctrl)
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)
	client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

	var onDoCalled, onSessionExecCalled int
	callTrace := &trace.Query{
		OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
			onDoCalled++

			return func(trace.QueryDoDoneInfo) {}
		},
		OnSessionExec: func(info trace.QuerySessionExecStartInfo) func(trace.QuerySessionExecDoneInfo) {
			onSessionExecCalled++

			return func(trace.QuerySessionExecDoneInfo) {}
		},
	}

	c := testClient(t, client)
	c.explicitSessionPool = &mockSessionPool{
		withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
			return f(ctx, newTestSessionWithClient("s-1", client, true))
		},
	}

	err := c.Do(ctx, func(ctx context.Context, s query.Session) error {
		return s.Exec(ctx, "SELECT 1")
	}, options.WithTrace(callTrace))
	require.NoError(t, err)
	require.Equal(t, 1, onDoCalled, "call-level OnDo must be invoked")
	require.Equal(t, 1, onSessionExecCalled, "call-level OnSessionExec must be invoked inside Do")
}
