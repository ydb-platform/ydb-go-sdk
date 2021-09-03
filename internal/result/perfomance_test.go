package result

import (
	"github.com/YandexDatabase/ydb-go-sdk/v3"
	"github.com/YandexDatabase/ydb-go-sdk/v3/internal"
	"testing"
	"time"
)

type series struct {
	id    uint64
	title string
	date  time.Time
}

var (
	testSize = 10000
)

func (s *series) UnmarshalYDB(res ydb.RawScanner) error {
	res.SeekItem("series_id")
	s.id = res.OUint64()
	res.SeekItem("title")
	s.title = res.OUTF8()
	res.SeekItem("release_date")
	s.date = internal.UnmarshalDatetime(res.ODatetime())
	return nil
}

func BenchmarkTestScanWithColumns(b *testing.B) {
	b.ReportAllocs()
	res := PrepareScannerPerformanceTest(b.N)
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
	res := PrepareScannerPerformanceTest(b.N)
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
	res := PrepareScannerPerformanceTest(b.N)
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
	res := PrepareScannerPerformanceTest(b.N)
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

func BenchmarkTestScanRow(b *testing.B) {
	b.ReportAllocs()
	res := PrepareScannerPerformanceTest(b.N)
	row := series{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res.NextRow() {
			res.ScanRaw(&row)
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
