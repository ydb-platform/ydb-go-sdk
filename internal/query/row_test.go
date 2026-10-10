package query

import (
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestRowScan(t *testing.T) {
	row := newDecodedRow([]*Ydb.Column{{Name: "id"}}, []value.Value{value.Int32Value(42)})
	for _, tt := range []struct {
		name    string
		method  string
		context string
		scan    func() error
	}{
		{
			name:    "indexed scan",
			method:  "Scan",
			context: "scan error on column index 0",
			scan: func() error {
				return row.Scan(new(bool))
			},
		},
		{
			name:    "named scan",
			method:  "ScanNamed",
			context: "scan error on column name 'id'",
			scan: func() error {
				return row.ScanNamed(scanner.NamedRef("id", new(bool)))
			},
		},
		{
			name:    "struct scan",
			method:  "ScanStruct",
			context: "scan error on struct field name 'id'",
			scan: func() error {
				return row.ScanStruct(&struct {
					ID bool `sql:"id"`
				}{})
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.scan()
			require.Error(t, err)
			require.ErrorIs(t, err, value.ErrCannotCast)
			require.ErrorContains(t, err, tt.context)
			require.ErrorContains(t, err, "cast failed 'Int32(42)' to '*bool' destination")
			require.ErrorContains(t, err, "github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Row)."+tt.method+"(")
			require.ErrorContains(t, err, "github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func")
		})
	}
}

func BenchmarkDecodedRow(b *testing.B) {
	columns := []*Ydb.Column{{Name: "id"}}
	values := []value.Value{value.Int32Value(42)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkRow = newDecodedRow(columns, values)
	}
}

var benchmarkRow *Row

func generateData(count int) []*Row {
	columns := []*Ydb.Column{{
		Name: "series_id",
		Type: &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: Ydb.Type_UINT64,
			},
		},
	}, {
		Name: "title",
		Type: &Ydb.Type{
			Type: &Ydb.Type_OptionalType{
				OptionalType: &Ydb.OptionalType{
					Item: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UTF8,
						},
					},
				},
			},
		},
	}, {
		Name: "release_date",
		Type: &Ydb.Type{
			Type: &Ydb.Type_OptionalType{
				OptionalType: &Ydb.OptionalType{
					Item: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_DATETIME,
						},
					},
				},
			},
		},
	}}
	rows := make([]*Row, count)
	for i := range count {
		rows[i] = NewRow(columns, &Ydb.Value{
			Items: []*Ydb.Value{{
				Value: &Ydb.Value_Uint64Value{
					Uint64Value: uint64(i),
				},
			}, {
				Value: &Ydb.Value_TextValue{
					TextValue: strconv.Itoa(i) + "a",
				},
			}, {
				Value: &Ydb.Value_Uint32Value{
					Uint32Value: uint32(i),
				},
			}},
		})
	}

	return rows
}

func BenchmarkScanner(b *testing.B) {
	// Keep calibration from changing the fixture size and heap pressure.
	const fixtureRows = 1024
	rows := generateData(fixtureRows)

	b.Run("Scan", func(b *testing.B) {
		b.ReportAllocs()
		var (
			id    uint64     // for requied scan
			title *string    // for optional scan
			date  *time.Time // for optional scan with default type value
		)
		for i := 0; i < b.N; i++ {
			if err := rows[i%fixtureRows].Scan(&id, &title, &date); err != nil {
				b.Error(err)
			}
		}
	})
	b.Run("ScanNamed", func(b *testing.B) {
		b.ReportAllocs()
		var (
			id    uint64     // for requied scan
			title *string    // for optional scan
			date  *time.Time // for optional scan with default type value
		)
		for i := 0; i < b.N; i++ {
			if err := rows[i%fixtureRows].ScanNamed(
				scanner.NamedRef("series_id", &id),
				scanner.NamedRef("title", &title),
				scanner.NamedRef("release_date", &date),
			); err != nil {
				b.Error(err)
			}
		}
	})
	b.Run("ScanStruct", func(b *testing.B) {
		b.ReportAllocs()
		var info struct {
			SeriesID    string     `sql:"series_id"`
			Title       *string    `sql:"title"`
			ReleaseDate *time.Time `sql:"release_date"`
		}
		for i := 0; i < b.N; i++ {
			if err := rows[i%fixtureRows].ScanStruct(&info); err != nil {
				b.Error(err)
			}
		}
	})
}

func TestReadRow(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)

		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
			ResultSetIndex: 0,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT64,
							},
						},
					},
				},
				Rows: []*Ydb.Value{
					{
						Items: []*Ydb.Value{{
							Value: &Ydb.Value_Uint64Value{
								Uint64Value: 42,
							},
						}},
					},
				},
			},
		}, nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)

		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings(), options.ResultSetsTypeConcurrent)
		require.NoError(t, err)

		row, err := readRow(ctx, r)
		require.NoError(t, err)
		require.NotNil(t, row)
	})

	t.Run("MoreThanOneRow", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)

		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
			ResultSetIndex: 0,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT64,
							},
						},
					},
				},
				Rows: []*Ydb.Value{
					{
						Items: []*Ydb.Value{{
							Value: &Ydb.Value_Uint64Value{
								Uint64Value: 42,
							},
						}},
					},
					{
						Items: []*Ydb.Value{{
							Value: &Ydb.Value_Uint64Value{
								Uint64Value: 43,
							},
						}},
					},
				},
			},
		}, nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)

		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings(), options.ResultSetsTypeConcurrent)
		require.NoError(t, err)

		_, err = readRow(ctx, r)
		require.ErrorIs(t, err, ErrMoreThanOneRow)
	})

	t.Run("NoRows", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)

		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
			ResultSetIndex: 0,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT64,
							},
						},
					},
				},
				Rows: []*Ydb.Value{},
			},
		}, nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)

		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings(), options.ResultSetsTypeConcurrent)
		require.NoError(t, err)

		_, err = readRow(ctx, r)
		require.ErrorIs(t, err, ErrNoRows)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("MoreThanOneResultSet", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)

		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
			ResultSetIndex: 0,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT64,
							},
						},
					},
				},
				Rows: []*Ydb.Value{
					{
						Items: []*Ydb.Value{{
							Value: &Ydb.Value_Uint64Value{
								Uint64Value: 42,
							},
						}},
					},
				},
			},
		}, nil)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 1,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{
					{
						Name: "b",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UTF8,
							},
						},
					},
				},
				Rows: []*Ydb.Value{},
			},
		}, nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)

		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings(), options.ResultSetsTypeConcurrent)
		require.NoError(t, err)

		_, err = readRow(ctx, r)
		require.ErrorIs(t, err, ErrMoreThanOneResultSet)
	})
}
