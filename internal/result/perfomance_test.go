package result

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/v2/internal"
	"strconv"
	"testing"
	"time"
)

func prepareTest(count int) *Scanner {
	res := Scanner{
		set: &Ydb.ResultSet{
			Columns:   nil,
			Rows:      nil,
			Truncated: false,
		},
		row: nil,
		stack: scanStack{
			v: [8]item{},
			p: 0,
		},
		nextRow:        0,
		nextItem:       0,
		setColumnIndex: nil,
		columnIndexes:  nil,
		err:            nil,
	}
	res.set.Columns = []*Ydb.Column{{
		Name: "series_id",
		Type: &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: Ydb.Type_UINT64,
			},
		},
	}, {
		Name: "title",
		Type: &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: Ydb.Type_UTF8,
			},
		},
	}, {
		Name: "release_date",
		Type: &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: Ydb.Type_DATETIME,
			},
		},
	}}
	n := count
	if n == 0 {
		n = testSize
	}
	res.set.Rows = []*Ydb.Value{}
	for i := 0; i < n; i++ {
		res.set.Rows = append(res.set.Rows, &Ydb.Value{
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
					Uint32Value: internal.MarshalDatetime(time.Now()) + uint32(i),
				},
			}},
		})
	}
	return &res
}

type series struct {
	id    uint64
	title string
	date  time.Time
}

var (
	testSize = 10000
	ar       []series
)

func BenchmarkTestScanWithColumns(b *testing.B) {
	b.ReportAllocs()
	res := prepareTest(b.N)
	row := series{}
	res.setColumnIndexes([]string{"series_id", "title", "release_date"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for res.NextRow() {
			res.Scan(&row.id, &row.title, &row.date)
		}
	}
}

func BenchmarkTestScan(b *testing.B) {
	b.ReportAllocs()
	res := prepareTest(b.N)
	row := series{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res.NextRow() {
			res.Scan(&row.id, &row.title, &row.date)
		}
	}
}

func BenchmarkTestDeprecatedNext(b *testing.B) {
	b.ReportAllocs()
	res := prepareTest(b.N)
	row := series{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res.NextRow() {
			res.NextItem()
			row.id = res.OUint64()
			res.NextItem()
			row.title = res.OUTF8()
			res.NextItem()
			row.date = internal.UnmarshalDatetime(res.ODatetime())
		}
	}
}

func BenchmarkTestDeprecatedSeek(b *testing.B) {
	b.ReportAllocs()
	res := prepareTest(b.N)
	row := series{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res.NextRow() {
			res.SeekItem("series_id")
			row.id = res.OUint64()
			res.SeekItem("title")
			row.title = res.OUTF8()
			res.SeekItem("release_date")
			row.date = internal.UnmarshalDatetime(res.ODatetime())
		}
	}
}

func TestOverallApproaches(t *testing.T) {
	for k, f := range map[string]func(b *testing.B){"BenchmarkTestScanWithColumns": BenchmarkTestScanWithColumns, "BenchmarkTestScan": BenchmarkTestScan, "BenchmarkTestDeprecatedSeek": BenchmarkTestDeprecatedSeek, "BenchmarkTestDeprecatedNext": BenchmarkTestDeprecatedNext} {
		r := testing.Benchmark(f)
		t.Log(k, r.String(), r.MemString())
	}
}

func TestOverallSliceApproaches(t *testing.T) {
	sizear := []int{2, 5, 10, 20, 50, 100}
	for _, testSize = range sizear {
		t.Logf("Slice size: %d", testSize)
		for _, test := range []struct {
			name string
			f    func(b *testing.B)
		}{
			{
				"BenchmarkTestDoubleIndex",
				BenchmarkTestDoubleIndex,
			}, {
				"BenchmarkTestTempValue",
				BenchmarkTestTempValue,
			},
		} {
			r := testing.Benchmark(test.f)
			t.Log(test.name, r.String())
		}
	}
}

func BenchmarkTestSliceReduce(b *testing.B) {
	var c = make([]*column, testSize, testSize)
	for j := 0; j < testSize; j++ {
		c[j] = &column{}
	}
	b.ResetTimer()
	var row column
	for i := 0; i < b.N; i++ {
		slice := c
		for j := 0; j < testSize; j++ {
			row = *slice[0]
			slice = slice[1:]
		}
	}
	_ = row
}

func BenchmarkTestSliceIncrement(b *testing.B) {
	var slice = make([]*column, testSize, testSize)
	for j := 0; j < testSize; j++ {
		slice[j] = &column{}
	}
	cnt := 0
	var row column
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cnt = 0
		for i := 0; i < testSize; i++ {
			row = *slice[cnt]
			cnt++
		}
	}
	_ = row
}

func BenchmarkTestTempValue(b *testing.B) {
	var slice = make([]*column, testSize, testSize)
	for j := 0; j < testSize; j++ {
		slice[j] = &column{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < testSize; i++ {
			col := slice[i]
			col.name = "test"
			col.typeID = 1
		}
	}
}

func BenchmarkTestDoubleIndex(b *testing.B) {
	var slice = make([]*column, testSize, testSize)
	for j := 0; j < testSize; j++ {
		slice[j] = &column{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < testSize; i++ {
			slice[i].name = "test"
			slice[i].typeID = 1
		}
	}
}
