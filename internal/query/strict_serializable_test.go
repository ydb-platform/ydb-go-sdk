package query

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/querytimestamp"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestPublicExecuteCommitTimestampCallback(t *testing.T) {
	service := NewMockQueryServiceClient(gomock.NewController(t))
	stream := newExecuteQueryStreamMock(gomock.NewController(t))
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:          Ydb.StatusIds_SUCCESS,
		CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 11, TxId: 12},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)
	service.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
			Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
		) {
			require.NotNil(t, request.GetTxControl().GetBeginTx().GetStrictSerializableReadWrite())
			require.True(t, request.GetTxControl().GetCommitTx())

			return stream, nil
		},
	)
	session := newTestSessionWithClient("s", service, false)
	session.databaseIdentity = querytimestamp.NewIdentity("/db")
	var got []*query.VirtualTimestamp
	result, err := session.Query(t.Context(), "UPSERT INTO t ...",
		query.WithTxControl(query.StrictSerializableReadWriteTxControl(query.CommitTx())),
		query.WithCommitTimestamp(func(timestamp *query.VirtualTimestamp) {
			got = append(got, timestamp)
		}),
	)
	require.NoError(t, err)
	provider := result.(query.CommitTimestampProvider)
	require.Nil(t, provider.CommitTimestamp())
	require.NoError(t, result.Close(t.Context()))
	require.Len(t, got, 1)
	require.Equal(t, uint64(11), got[0].PlanStep())
	require.Equal(t, "/db", got[0].Database())
	require.Equal(t, got[0], provider.CommitTimestamp())
}

func TestMaterializedResultRetainsCommitTimestamp(t *testing.T) {
	stream := newExecuteQueryStreamMock(gomock.NewController(t))
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:          Ydb.StatusIds_SUCCESS,
		CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 3, TxId: 4},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)
	r, err := newResult(t.Context(), stream, withStreamResultIdentity(querytimestamp.NewIdentity("/db")))
	require.NoError(t, err)
	materialized, err := resultToMaterializedResult(t.Context(), r)
	require.NoError(t, err)
	provider := materialized.(query.CommitTimestampProvider)
	require.Equal(t, uint64(3), provider.CommitTimestamp().PlanStep())
	require.Equal(t, uint64(4), provider.CommitTimestamp().TxID())
}

func TestTransactionExecuteWithCommitTimestamp(t *testing.T) {
	service := NewMockQueryServiceClient(gomock.NewController(t))
	stream := newExecuteQueryStreamMock(gomock.NewController(t))
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:          Ydb.StatusIds_SUCCESS,
		CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 21, TxId: 22},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)
	service.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
			Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
		) {
			require.Equal(t, "tx", request.GetTxControl().GetTxId())
			require.True(t, request.GetTxControl().GetCommitTx())

			return stream, nil
		},
	)
	session := newTestSessionWithClient("s", service, false)
	session.databaseIdentity = querytimestamp.NewIdentity("/db")
	tx := &Transaction{
		s: session, LazyID: baseTx.ID("tx"),
		txSettings: query.TxSettings(query.WithStrictSerializableReadWrite()),
	}
	result, err := tx.Query(t.Context(), "UPSERT INTO t ...", query.WithCommit())
	require.NoError(t, err)
	require.Nil(t, tx.CommitTimestamp())
	require.NoError(t, result.Close(t.Context()))
	require.Equal(t, uint64(21), tx.CommitTimestamp().PlanStep())
	require.Equal(t, uint64(22), tx.CommitTimestamp().TxID())
	require.Equal(t, "/db", tx.CommitTimestamp().Database())
}

func TestStrictSerializableRWMode(t *testing.T) {
	settings := query.TxSettings(query.WithStrictSerializableReadWrite()).ToYdbQuerySettings()
	require.IsType(t, &Ydb_Query.TransactionSettings_StrictSerializableReadWrite{}, settings.GetTxMode())
	require.NotNil(t, settings.GetStrictSerializableReadWrite())
	field := settings.ProtoReflect().Descriptor().Fields().ByName("strict_serializable_read_write")
	require.Equal(t, protoreflect.FieldNumber(7), field.Number())
	require.PanicsWithValue(t, "StrictSerializableRW is supported only by Query Service", func() {
		query.TxSettings(query.WithStrictSerializableReadWrite()).ToYdbTableSettings()
	})

	control := query.StrictSerializableReadWriteTxControl(query.CommitTx())
	require.True(t, control.Commit())
	require.IsType(t, &Ydb_Query.TransactionSettings_StrictSerializableReadWrite{},
		control.ToYdbQueryTransactionControl().GetBeginTx().GetTxMode())
	require.False(t, control.IsBeginTxWithoutCommit())
	require.True(t, query.StrictSerializableReadWriteTxControl().IsBeginTxWithoutCommit())

	request, _, err := executeQueryRequest("s", "UPSERT INTO t ...", options.ExecuteSettings(
		options.WithTxControl(control),
	), options.ResultSetsTypeOrdered)
	require.NoError(t, err)
	require.IsType(t, &Ydb_Query.TransactionSettings_StrictSerializableReadWrite{},
		request.GetTxControl().GetBeginTx().GetTxMode())

	// The existing default remains the original serializable mode.
	require.IsType(t, &Ydb_Query.TransactionSettings_SerializableReadWrite{},
		baseTx.NewControl().ToYdbQueryTransactionControl().GetBeginTx().GetTxMode())
}

func TestCommitTransactionTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name          string
		response      *Ydb_Query.CommitTransactionResponse
		wantTimestamp bool
	}{
		{"present", &Ydb_Query.CommitTransactionResponse{
			Status:          Ydb.StatusIds_SUCCESS,
			CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 10, TxId: 20},
		}, true},
		{"absent", &Ydb_Query.CommitTransactionResponse{Status: Ydb.StatusIds_SUCCESS}, false},
		{"unsuccessful", &Ydb_Query.CommitTransactionResponse{
			Status:          Ydb.StatusIds_ABORTED,
			CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 10, TxId: 20},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := NewMockQueryServiceClient(gomock.NewController(t))
			service.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(tc.response, nil)
			session := newTestSessionWithClient("s", service, false)
			session.databaseIdentity = querytimestamp.NewIdentity("/db")
			tx := &Transaction{
				s: session, LazyID: baseTx.ID("tx"),
				txSettings: query.TxSettings(query.WithStrictSerializableReadWrite()),
			}
			require.NoError(t, tx.CommitTx(t.Context()))
			var provider query.CommitTimestampProvider = tx
			if tc.wantTimestamp {
				require.Equal(t, uint64(10), provider.CommitTimestamp().PlanStep())
				require.Equal(t, uint64(20), provider.CommitTimestamp().TxID())
				require.Equal(t, "/db", provider.CommitTimestamp().Database())
			} else {
				require.Nil(t, provider.CommitTimestamp())
			}
		})
	}
}

func TestExecuteQueryCommitTimestampFromTrailingPart(t *testing.T) {
	for _, tc := range []struct {
		name          string
		parts         []*Ydb_Query.ExecuteQueryResponsePart
		terminalErr   error
		wantTimestamp bool
	}{
		{"trailing", []*Ydb_Query.ExecuteQueryResponsePart{
			{Status: Ydb.StatusIds_SUCCESS},
			{Status: Ydb.StatusIds_SUCCESS, CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 7, TxId: 9}},
		}, io.EOF, true},
		{"not trailing", []*Ydb_Query.ExecuteQueryResponsePart{
			{Status: Ydb.StatusIds_SUCCESS, CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 7, TxId: 9}},
			{Status: Ydb.StatusIds_SUCCESS},
		}, io.EOF, false},
		{"absent", []*Ydb_Query.ExecuteQueryResponsePart{{Status: Ydb.StatusIds_SUCCESS}}, io.EOF, false},
		{"unsuccessful", []*Ydb_Query.ExecuteQueryResponsePart{{
			Status: Ydb.StatusIds_ABORTED, CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 7, TxId: 9},
		}}, io.EOF, false},
		{"stream error", []*Ydb_Query.ExecuteQueryResponsePart{{
			Status: Ydb.StatusIds_SUCCESS, CommitTimestamp: &Ydb.VirtualTimestamp{PlanStep: 7, TxId: 9},
		}}, errors.New("stream failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := newExecuteQueryStreamMock(gomock.NewController(t))
			for _, part := range tc.parts {
				stream.EXPECT().Recv().Return(part, nil)
			}
			stream.EXPECT().Recv().Return(nil, tc.terminalErr)
			var callbacks []*query.VirtualTimestamp
			r, err := newResult(t.Context(), stream,
				withStreamResultIdentity(querytimestamp.NewIdentity("/db")),
				withStreamResultCommitTimestampCallback(func(ts *query.VirtualTimestamp) {
					callbacks = append(callbacks, ts)
				}),
			)
			require.NoError(t, err)
			require.Nil(t, r.CommitTimestamp())
			closeErr := r.Close(t.Context())
			if errors.Is(tc.terminalErr, io.EOF) {
				require.NoError(t, closeErr)
			} else {
				require.Error(t, closeErr)
			}
			if tc.wantTimestamp {
				require.Len(t, callbacks, 1)
				require.Equal(t, uint64(7), r.CommitTimestamp().PlanStep())
				require.Equal(t, uint64(9), r.CommitTimestamp().TxID())
			} else {
				require.Empty(t, callbacks)
				require.Nil(t, r.CommitTimestamp())
			}
		})
	}
}
