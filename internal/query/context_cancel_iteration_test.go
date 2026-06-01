package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"google.golang.org/protobuf/proto"
)

func testPartOneResultSetTwoRows(t *testing.T) *Ydb_Query.ExecuteQueryResponsePart {
	t.Helper()

	return Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{Ydb.Column_builder{
				Name: "id",
				Type: Ydb.Type_builder{TypeId: Ydb.Type_INT64.Enum()}.Build(),
			}.Build()},
			Rows: []*Ydb.Value{Ydb.Value_builder{
				Items: []*Ydb.Value{Ydb.Value_builder{Int64Value: proto.Int64(10)}.Build()},
			}.Build(), Ydb.Value_builder{
				Items: []*Ydb.Value{Ydb.Value_builder{Int64Value: proto.Int64(20)}.Build()},
			}.Build()},
		}.Build(),
	}.Build()
}

func TestStreamResult_NextResultSet_ContextCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := NewMockQueryService_ExecuteQueryClient(ctrl)

	stream.EXPECT().Recv().Return(testPartOneResultSetTwoRows(t), nil).Times(1)
	stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

	bg := context.Background()

	r, err := newResult(bg, stream)
	require.NoError(t, err)
	defer func() {
		_ = r.Close(bg)
	}()

	iterCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = r.NextResultSet(iterCtx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamResult_NextRow_ContextCanceledAfterFirstRow(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := NewMockQueryService_ExecuteQueryClient(ctrl)

	stream.EXPECT().Recv().Return(testPartOneResultSetTwoRows(t), nil).Times(1)
	stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

	bg := context.Background()

	r, err := newResult(bg, stream)
	require.NoError(t, err)
	defer func() {
		_ = r.Close(bg)
	}()

	rs, err := r.NextResultSet(bg)
	require.NoError(t, err)
	require.NotNil(t, rs)

	rowCtx, cancel := context.WithCancel(bg)
	_, err = rs.NextRow(rowCtx)
	require.NoError(t, err)

	cancel()

	_, err = rs.NextRow(rowCtx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamResult_NextResultSet_AfterDrain_ContextCanceledNotEOF(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := NewMockQueryService_ExecuteQueryClient(ctrl)

	part := testPartOneResultSetTwoRows(t)

	stream.EXPECT().Recv().Return(part, nil).Times(1)
	stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

	bg := context.Background()

	r, err := newResult(bg, stream)
	require.NoError(t, err)

	rs, err := r.NextResultSet(bg)
	require.NoError(t, err)

	for {
		_, err := rs.NextRow(bg)
		if xerrors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
	}

	rowCtx, cancel := context.WithCancel(bg)
	cancel()

	_, err = r.NextResultSet(rowCtx)
	require.ErrorIs(t, err, context.Canceled)

	_ = r.Close(bg)
}
