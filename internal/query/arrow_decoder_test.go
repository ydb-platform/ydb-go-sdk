package query

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Formats"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestArrowResults(t *testing.T) {
	for _, materialized := range []bool{false, true} {
		t.Run(map[bool]string{false: "streaming", true: "materialized"}[materialized], func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			columns := arrowTestColumns()
			parts := []*Ydb_Query.ExecuteQueryResponsePart{
				arrowTestPart(0, columns, "1"),
				arrowTestPart(0, nil, ""),
				arrowTestPart(0, nil, "2"),
				arrowTestPart(1, columns, "3"),
			}
			if materialized {
				parts[2], parts[3] = parts[3], parts[2]
			}
			for _, part := range parts {
				stream.EXPECT().Recv().Return(part, nil)
			}
			stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			calls := 0
			decoder := func(ctx context.Context, columns []query.ArrowColumn, ipc io.Reader) ([][]types.Value, error) {
				require.NoError(t, ctx.Err())
				require.Equal(t, "id", columns[0].Name)
				require.True(t, types.Equal(types.TypeInt32, columns[0].Type))
				require.True(t, types.Equal(types.Optional(types.TypeText), columns[1].Type))
				data, err := io.ReadAll(ipc)
				require.NoError(t, err)
				require.Len(t, data, 2)
				require.Equal(t, byte('s'), data[0])
				calls++

				return [][]types.Value{{types.Int32Value(int32(data[1] - '0')), types.NullValue(types.TypeText)}}, nil
			}
			r, err := newResult(ctx, stream, withArrowDecoder(decoder))
			require.NoError(t, err)
			var result query.Result = r
			if materialized {
				result, err = resultToMaterializedResult(ctx, r)
				require.NoError(t, err)
			}
			var retained []query.Row
			for rs, err := range result.ResultSets(ctx) {
				require.NoError(t, err)
				require.Equal(t, []string{"id", "name"}, rs.Columns())
				for row, err := range rs.Rows(ctx) {
					require.NoError(t, err)
					retained = append(retained, row)
				}
			}
			require.NoError(t, result.Close(ctx))
			require.NoError(t, r.Close(ctx))
			require.Equal(t, 3, calls)
			for i, row := range retained {
				var id int32
				var name *string
				require.NoError(t, row.Scan(&id, &name))
				require.Equal(t, int32(i+1), id)
				require.Nil(t, name)
				require.NoError(t, row.ScanNamed(query.Named("id", &id)))
				dst := struct {
					ID   int32   `db:"id"`
					Name *string `db:"name"`
				}{}
				require.NoError(t, row.ScanStruct(&dst, query.WithScanStructTagName("db")))
				require.Equal(t, id, dst.ID)
				values := row.Values()
				require.Len(t, values, 2)
				values[0] = types.Int32Value(999)
				require.NoError(t, row.ScanNamed(query.Named("id", &id)))
				require.Equal(t, int32(i+1), id)
			}
		})
	}
}

func TestArrowDecoderErrors(t *testing.T) {
	decodeErr := errors.New("invalid IPC")
	for _, test := range []struct {
		name    string
		decoder query.ArrowDecoder
		want    string
	}{
		{name: "missing", want: "without an Arrow decoder"},
		{name: "decode", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([][]types.Value, error) {
			return nil, decodeErr
		}, want: "invalid IPC"},
		{name: "EOF", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([][]types.Value, error) {
			return nil, io.EOF
		}, want: "unexpected EOF"},
		{name: "width", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([][]types.Value, error) {
			return [][]types.Value{{types.Int32Value(1)}}, nil
		}, want: "expected 2"},
		{name: "nil", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([][]types.Value, error) {
			return [][]types.Value{{nil, nil}}, nil
		}, want: "nil value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(arrowTestPart(0, arrowTestColumns(), "1"), nil)
			stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			var notified error
			r, err := newResult(t.Context(), stream,
				withArrowDecoder(test.decoder), onNextPartErr(func(err error) { notified = err }),
			)
			require.NoError(t, err)
			rs, err := r.NextResultSet(t.Context())
			require.NoError(t, err)
			_, err = rs.NextRow(t.Context())
			require.ErrorContains(t, err, test.want)
			require.NotErrorIs(t, err, io.EOF)
			require.ErrorContains(t, notified, test.want)
			_, err = rs.NextRow(t.Context())
			require.ErrorIs(t, err, io.EOF)
			require.NoError(t, r.Close(t.Context()))
		})
	}
}

func TestArrowQueryRowConstraints(t *testing.T) {
	for _, test := range []struct {
		name       string
		rows       int
		anotherSet bool
		want       error
	}{
		{name: "empty", want: ErrNoRows},
		{name: "one", rows: 1},
		{name: "multiple rows", rows: 2, want: ErrMoreThanOneRow},
		{name: "multiple sets", rows: 1, anotherSet: true, want: ErrMoreThanOneResultSet},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(arrowTestPart(0, arrowTestColumns(), "data"), nil)
			if test.anotherSet {
				stream.EXPECT().Recv().Return(arrowTestPart(1, arrowTestColumns(), "data"), nil)
			}
			stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			decoder := func(context.Context, []query.ArrowColumn, io.Reader) ([][]types.Value, error) {
				rows := make([][]types.Value, test.rows)
				for i := range rows {
					rows[i] = []types.Value{types.Int32Value(1), types.NullValue(types.TypeText)}
				}

				return rows, nil
			}
			r, err := newResult(t.Context(), stream, withArrowDecoder(decoder))
			require.NoError(t, err)
			row, err := readRow(t.Context(), r)
			if test.want != nil {
				require.ErrorIs(t, err, test.want)
			} else {
				require.NoError(t, err)
				require.Len(t, row.Values(), 2)
			}
		})
	}
}

func TestArrowSkipAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip", true: "cancel"}[cancelled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(arrowTestPart(0, arrowTestColumns(), "unread"), nil)
			if !cancelled {
				stream.EXPECT().Recv().Return(arrowTestPart(0, nil, "also unread"), nil)
				stream.EXPECT().Recv().Return(arrowTestPart(1, arrowTestColumns(), "read"), nil)
			}
			stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			calls := 0
			decoder := func(context.Context, []query.ArrowColumn, io.Reader) ([][]types.Value, error) {
				calls++

				return [][]types.Value{{types.Int32Value(1), types.NullValue(types.TypeText)}}, nil
			}
			r, err := newResult(ctx, stream, withArrowDecoder(decoder))
			require.NoError(t, err)
			rs, err := r.NextResultSet(ctx)
			require.NoError(t, err)
			if cancelled {
				cancel()
				_, err = rs.NextRow(ctx)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 0, calls)
			} else {
				rs, err = r.NextResultSet(ctx)
				require.NoError(t, err)
				require.Equal(t, 1, rs.Index())
				_, err = rs.NextRow(ctx)
				require.NoError(t, err)
				require.Equal(t, 1, calls)
			}
			require.NoError(t, r.Close(t.Context()))
		})
	}
}

func TestArrowExecuteOptionDefaults(t *testing.T) {
	decoder := query.ArrowDecoder(func(context.Context, []query.ArrowColumn, io.Reader) ([][]types.Value, error) {
		return nil, nil
	})
	cfg := config.New(config.WithDefaultExecuteOptions(query.WithArrow(decoder)))
	c := Client{config: cfg}
	s := Session{defaultExecuteOptions: cfg.DefaultExecuteOptions()}
	tx := Transaction{s: &s}
	for _, opts := range [][]query.ExecuteOption{nil, {query.WithArrow(nil)}} {
		expected := Ydb.ResultSet_FORMAT_ARROW
		if len(opts) > 0 {
			expected = Ydb.ResultSet_FORMAT_UNSPECIFIED
		}
		clientSettings := options.ExecuteSettings(c.withDefaultExecuteOptions(opts...)...)
		sessionSettings := options.ExecuteSettings(s.withDefaultExecuteOptions(opts...)...)
		txSettings, err := tx.executeSettings(opts...)
		require.NoError(t, err)
		for _, settings := range []executeSettings{clientSettings, sessionSettings, txSettings} {
			request, _, err := executeQueryRequest("session", "SELECT 1", settings, options.ResultSetsTypeOrdered)
			require.NoError(t, err)
			require.Equal(t, expected, request.GetResultSetFormat())
		}
	}
}

func arrowTestColumns() []*Ydb.Column {
	return []*Ydb.Column{
		{Name: "id", Type: types.TypeInt32.ToYDB()},
		{Name: "name", Type: types.Optional(types.TypeText).ToYDB()},
	}
}

func arrowTestPart(index int64, columns []*Ydb.Column, data string) *Ydb_Query.ExecuteQueryResponsePart {
	return &Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS, ResultSetIndex: index,
		ResultSet: &Ydb.ResultSet{
			Format: Ydb.ResultSet_FORMAT_ARROW, Columns: columns,
			ArrowFormatMeta: &Ydb_Formats.ArrowFormatMeta{Schema: []byte("s")}, Data: []byte(data),
		},
	}
}
