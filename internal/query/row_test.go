package query

import (
	"errors"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

type testScanner struct {
	err error
}

func (s testScanner) Scan(dst ...interface{}) error {
	return s.err
}

func (s testScanner) ScanNamed(dst ...scanner.NamedDestination) error {
	return s.err
}

func (s testScanner) ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) error {
	return s.err
}

func TestRowScan(t *testing.T) {
	expErr := errors.New("test error")
	row := Row{
		indexedScanner: testScanner{err: expErr},
		namedScanner:   testScanner{err: expErr},
		structScanner:  testScanner{err: expErr},
	}
	for _, tt := range []struct {
		name      string
		scan      func() error
		expErrStr string
	}{
		{
			name: "indexed scan",
			scan: func() error {
				return row.Scan()
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.Scan(row.go:50)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func1(row_test.go:51)`", //nolint:lll
		},
		{
			name: "named scan",
			scan: func() error {
				return row.ScanNamed()
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.ScanNamed(row.go:62)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func2(row_test.go:58)`", //nolint:lll
		},
		{
			name: "struct scan",
			scan: func() error {
				return row.ScanStruct(nil)
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.ScanStruct(row.go:74)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func3(row_test.go:65)`", //nolint:lll
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.scan()
			require.Error(t, err)
			require.ErrorIs(t, err, expErr)
			require.Equal(t, tt.expErrStr, err.Error())
		})
	}
}

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
	for i := 0; i < count; i++ {
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
	b.Run("Scan", func(b *testing.B) {
		b.ReportAllocs()
		rows := generateData(b.N)
		var (
			id    uint64     // for requied scan
			title *string    // for optional scan
			date  *time.Time // for optional scan with default type value
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := rows[i].Scan(&id, &title, &date); err != nil {
				b.Error(err)
			}
		}
	})
	b.Run("ScanNamed", func(b *testing.B) {
		b.ReportAllocs()
		rows := generateData(b.N)
		var (
			id    uint64     // for requied scan
			title *string    // for optional scan
			date  *time.Time // for optional scan with default type value
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := rows[i].ScanNamed(
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
		rows := generateData(b.N)
		var info struct {
			SeriesID    string     `sql:"series_id"`
			Title       *string    `sql:"title"`
			ReleaseDate *time.Time `sql:"release_date"`
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := rows[i].ScanStruct(&info); err != nil {
				b.Error(err)
			}
		}
	})
}

func TestReadRow(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)

		// Создаем stream с ровно одной строкой
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
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

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings())
		require.NoError(t, err)

		row, err := readRow(ctx, r)
		require.NoError(t, err)
		require.NotNil(t, row)
	})

	t.Run("MoreThanOneRow", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)

		// Создаем stream с двумя строками
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
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

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings())
		require.NoError(t, err)

		_, err = readRow(ctx, r)
		require.ErrorIs(t, err, ErrMoreThanOneRow)
	})

	t.Run("NoRows", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)

		// Создаем stream без строк
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
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

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings())
		require.NoError(t, err)

		_, err = readRow(ctx, r)
		require.ErrorIs(t, err, ErrNoRows)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("MoreThanOneResultSet", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)

		// Создаем stream с двумя result set'ами
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
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

		r, err := execute(ctx, "123", client, "", options.ExecuteSettings())
		require.NoError(t, err)

		_, err = readRow(ctx, r)
		require.ErrorIs(t, err, ErrMoreThanOneResultSet)
	})
}
