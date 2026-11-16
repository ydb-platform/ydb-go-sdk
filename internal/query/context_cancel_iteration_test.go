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
)

func testPartOneResultSetTwoRows(t *testing.T) *Ydb_Query.ExecuteQueryResponsePart {
	t.Helper()

	return &Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{{
				Name: "id",
				Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}},
			}},
			Rows: []*Ydb.Value{{
				Items: []*Ydb.Value{{Value: &Ydb.Value_Int64Value{Int64Value: 10}}},
			}, {
				Items: []*Ydb.Value{{Value: &Ydb.Value_Int64Value{Int64Value: 20}}},
			}},
		},
	}
}

func TestStreamResult_NextResultSet_ContextCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)

	stream.EXPECT().Recv().Return(testPartOneResultSetTwoRows(t), nil).Times(1)
	stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

	bg := t.Context()

	r, err := newResult(bg, stream)
	require.NoError(t, err)
	defer func() {
		_ = r.Close(bg)
	}()

	iterCtx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = r.NextResultSet(iterCtx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamResult_NextRow_ContextCanceledAfterFirstRow(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)

	stream.EXPECT().Recv().Return(testPartOneResultSetTwoRows(t), nil).Times(1)
	stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

	bg := t.Context()

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
	stream := newExecuteQueryStreamMock(ctrl)

	part := testPartOneResultSetTwoRows(t)

	stream.EXPECT().Recv().Return(part, nil).Times(1)
	stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

	bg := t.Context()

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
