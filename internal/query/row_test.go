package query

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
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
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.Scan(row.go:39)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func1(row_test.go:46)`", //nolint:lll
		},
		{
			name: "named scan",
			scan: func() error {
				return row.ScanNamed()
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.ScanNamed(row.go:51)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func2(row_test.go:53)`", //nolint:lll
		},
		{
			name: "struct scan",
			scan: func() error {
				return row.ScanStruct(nil)
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.ScanStruct(row.go:63)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func3(row_test.go:60)`", //nolint:lll
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
