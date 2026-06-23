//go:build go1.23

package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"google.golang.org/protobuf/proto"
)

func TestResultRangeResultSets(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "a",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "b",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(1),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("1"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(2),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("2"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(3),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("3"),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: Ydb.ResultSet_builder{
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(4),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("4"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(5),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("5"),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 1,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "c",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "d",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "e",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_BOOL.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(1),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("1"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(2),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("2"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 1,
		ResultSet: Ydb.ResultSet_builder{
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(3),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("3"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(4),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("4"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(5),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("5"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 2,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "c",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "d",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "e",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_BOOL.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(1),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("1"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(2),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("2"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 2,
		ResultSet: Ydb.ResultSet_builder{
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(3),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("3"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(4),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("4"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(5),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("5"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)
	r, err := newResult(ctx, stream, nil)
	require.NoError(t, err)
	defer r.Close(ctx)
	rsCount := 0
	for rs, err := range r.ResultSets(ctx) {
		require.NoError(t, err)
		rowsCount := 0
		for _, err := range rs.Rows(ctx) {
			require.NoError(t, err)
			rowsCount++
		}
		require.EqualValues(t, 5, rowsCount)
		rsCount++
	}
	require.EqualValues(t, 3, rsCount)
}
