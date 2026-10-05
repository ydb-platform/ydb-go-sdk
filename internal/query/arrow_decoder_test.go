package query

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Formats"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

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
			var batches []*arrowTestBatch
			decoder := func(ctx context.Context, columns []query.ArrowColumn, ipc io.Reader) ([]query.ArrowBatch, error) {
				require.NoError(t, ctx.Err())
				require.Equal(t, "id", columns[0].Name)
				require.True(t, types.Equal(types.TypeInt32, columns[0].Type))
				require.True(t, types.Equal(types.Optional(types.TypeText), columns[1].Type))
				data, err := io.ReadAll(ipc)
				require.NoError(t, err)
				require.Len(t, data, 2)
				require.Equal(t, byte('s'), data[0])
				calls++

				batch := &arrowTestBatch{rows: [][]types.Value{
					{types.Int32Value(int32(data[1] - '0')), types.NullValue(types.TypeText)},
				}}
				batches = append(batches, batch)

				return []query.ArrowBatch{batch}, nil
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
					if !materialized {
						verifyArrowTestRow(t, row, int32(len(retained)))
					}
				}
			}
			require.Equal(t, 3, calls)
			if materialized {
				for i, row := range retained {
					verifyArrowTestRow(t, row, int32(i+1))
				}
			}
			for _, batch := range batches {
				if materialized {
					require.Zero(t, batch.releases)
				} else {
					require.Equal(t, 1, batch.releases)
				}
				require.Zero(t, batch.valueCalls-2)
			}
			require.NoError(t, result.Close(ctx))
			require.NoError(t, r.Close(ctx))
			for _, batch := range batches {
				require.Equal(t, 1, batch.releases)
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
		{name: "decode", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
			return nil, decodeErr
		}, want: "invalid IPC"},
		{name: "EOF", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
			return nil, io.EOF
		}, want: "unexpected EOF"},
		{name: "width", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
			return arrowTestBatches([][]types.Value{{types.Int32Value(1)}}), nil
		}, want: "expected 2"},
		{name: "nil", decoder: func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
			return []query.ArrowBatch{nil}, nil
		}, want: "is nil"},
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

func TestArrowDecoderErrorContext(t *testing.T) {
	decodeErr := errors.New("invalid IPC")
	for _, materialized := range []bool{false, true} {
		t.Run(map[bool]string{false: "streaming", true: "materialized"}[materialized], func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS, ResultSet: &Ydb.ResultSet{Columns: arrowTestColumns()},
			}, nil)
			stream.EXPECT().Recv().Return(arrowTestPart(1, arrowTestColumns(), "data"), nil)
			stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			decoder := func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
				return nil, decodeErr
			}
			var notified error
			r, err := newResult(ctx, stream,
				withArrowDecoder(decoder), onNextPartErr(func(err error) { notified = err }),
			)
			require.NoError(t, err)
			if materialized {
				_, err = resultToMaterializedResult(ctx, r)
			} else {
				var rs query.ResultSet
				rs, err = r.NextResultSet(ctx)
				require.NoError(t, err)
				_, err = rs.NextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
				rs, err = r.NextResultSet(ctx)
				require.NoError(t, err)
				_, err = rs.NextRow(ctx)
			}
			require.ErrorContains(t, err, "arrow result set 1")
			require.ErrorIs(t, err, decodeErr)
			require.ErrorContains(t, notified, "arrow result set 1")
			require.ErrorIs(t, notified, decodeErr)
			require.NoError(t, r.Close(ctx))
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
			decoder := func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
				rows := make([][]types.Value, test.rows)
				for i := range rows {
					rows[i] = []types.Value{types.Int32Value(1), types.NullValue(types.TypeText)}
				}

				return arrowTestBatches(rows), nil
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
			decoder := func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
				calls++

				return arrowTestBatches([][]types.Value{{types.Int32Value(1), types.NullValue(types.TypeText)}}), nil
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
	decoder := query.ArrowDecoder(func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
		return nil, nil
	})
	for _, defaults := range []struct {
		name  string
		opts  []config.Option
		arrow bool
	}{
		{name: "unset"},
		{name: "Arrow", opts: []config.Option{config.WithDefaultResultFormatArrow(decoder)}, arrow: true},
		{name: "reset", opts: []config.Option{
			config.WithDefaultResultFormatArrow(decoder), config.WithDefaultResultFormatArrow(nil),
		}},
	} {
		t.Run(defaults.name, func(t *testing.T) {
			cfg := config.New(defaults.opts...)
			c := Client{config: cfg}
			s := Session{defaultArrowDecoder: cfg.DefaultArrowDecoder()}
			tx := Transaction{s: &s}
			for _, call := range []struct {
				name  string
				opts  []query.ExecuteOption
				arrow bool
			}{
				{name: "default", arrow: defaults.arrow},
				{name: "Ydb.Value", opts: []query.ExecuteOption{query.WithArrow(nil)}},
				{name: "Arrow", opts: []query.ExecuteOption{query.WithArrow(decoder)}, arrow: true},
			} {
				t.Run(call.name, func(t *testing.T) {
					expected := Ydb.ResultSet_FORMAT_UNSPECIFIED
					if call.arrow {
						expected = Ydb.ResultSet_FORMAT_ARROW
					}
					clientSettings := options.ExecuteSettings(c.withDefaultExecuteOptions(call.opts...)...)
					sessionSettings := options.ExecuteSettings(s.withDefaultExecuteOptions(call.opts...)...)
					txSettings, err := tx.executeSettings(call.opts...)
					require.NoError(t, err)
					for _, settings := range []executeSettings{clientSettings, sessionSettings, txSettings} {
						request, _, err := executeQueryRequest("session", "SELECT 1", settings, options.ResultSetsTypeOrdered)
						require.NoError(t, err)
						require.Equal(t, expected, request.GetResultSetFormat())
					}
				})
			}
		})
	}
}

func TestArrowExec(t *testing.T) {
	decoder := query.ArrowDecoder(func(context.Context, []query.ArrowColumn, io.Reader) ([]query.ArrowBatch, error) {
		return nil, errors.New("Exec must not invoke the decoder")
	})
	for _, executor := range []string{"Client", "Session", "TxActor"} {
		t.Run(executor, func(t *testing.T) {
			for _, override := range []bool{false, true} {
				t.Run(map[bool]string{false: "default", true: "Ydb.Value"}[override], func(t *testing.T) {
					ctrl := gomock.NewController(t)
					stream := newExecuteQueryStreamMock(ctrl)
					format := Ydb.ResultSet_FORMAT_ARROW
					part := arrowTestPart(0, arrowTestColumns(), "discarded IPC")
					var opts []query.ExecuteOption
					if override {
						format = Ydb.ResultSet_FORMAT_UNSPECIFIED
						part.ResultSet = &Ydb.ResultSet{Columns: arrowTestColumns()}
						opts = append(opts, query.WithArrow(nil))
					}
					stream.EXPECT().Recv().Return(part, nil)
					stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
					client := NewMockQueryServiceClient(ctrl)
					client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
						func(_ context.Context, req *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
							Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
						) {
							require.Equal(t, format, req.GetResultSetFormat())

							return stream, nil
						},
					)
					s := newTestSessionWithClient("session", client, false)
					s.defaultArrowDecoder = decoder
					var e query.Executor
					switch executor {
					case "Client":
						c := testClient(t, client)
						c.config = config.New(config.WithDefaultResultFormatArrow(decoder))
						p := &mockSessionPool{withFunc: func(ctx context.Context, f func(context.Context, *Session) error) error {
							return f(ctx, s)
						}}
						c.explicitSessionPool, c.implicitSessionPool = p, p
						defer c.Close(t.Context())
						e = c
					case "Session":
						e = s
					case "TxActor":
						tx := &Transaction{s: s}
						tx.SetTxID("transaction")
						e = tx
					}
					require.NoError(t, e.Exec(t.Context(), "SELECT 1", opts...))
				})
			}
		})
	}
}

func verifyArrowTestRow(t *testing.T, row query.Row, expected int32) {
	t.Helper()
	var id int32
	var name *string
	require.NoError(t, row.Scan(&id, &name))
	require.Equal(t, expected, id)
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
	require.Equal(t, expected, id)
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

type arrowTestBatch struct {
	rows       [][]types.Value
	releases   int
	valueCalls int
}

func arrowTestBatches(rows [][]types.Value) []query.ArrowBatch {
	return []query.ArrowBatch{&arrowTestBatch{rows: rows}}
}
func (b *arrowTestBatch) NumRows() int { return len(b.rows) }
func (b *arrowTestBatch) NumCols() int {
	if len(b.rows) == 0 {
		return 2
	}

	return len(b.rows[0])
}

func (b *arrowTestBatch) Scan(row, column int, dst any) error {
	return types.CastTo(b.rows[row][column], dst)
}

func (b *arrowTestBatch) Value(row, column int) types.Value {
	b.valueCalls++

	return b.rows[row][column]
}
func (b *arrowTestBatch) Release() { b.rows = nil; b.releases++ }
