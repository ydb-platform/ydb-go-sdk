package scanner

import (
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

var testSize = 10000

func BenchmarkTestScanWithColumns(b *testing.B) {
	b.ReportAllocs()
	res := generateScannerData(b.N)
	var (
		id    uint64     // for requied scan
		title *string    // for optional scan
		date  *time.Time // for optional scan with default type value
	)
	res.setColumnIndexes([]string{"series_id", "title", "release_date"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for res.NextRow() {
			if err := res.Scan(&id, &title, &date); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTestScan(b *testing.B) {
	b.ReportAllocs()
	res := generateScannerData(b.N)
	var (
		id    uint64     // for requied scan
		title *string    // for optional scan
		date  *time.Time // for optional scan with default type value
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res.NextRow() {
			if err := res.Scan(&id, &title, &date); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTestScanNamed(b *testing.B) {
	b.ReportAllocs()
	res := generateScannerData(b.N)
	var (
		id    uint64    // for requied scan
		title *string   // for optional scan
		date  time.Time // for optional scan with default type value
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for res.NextRow() {
			if err := res.ScanNamed(
				named.Required("series_id", &id),
				named.Optional("title", &title),
				named.OptionalWithDefault("release_date", &date),
			); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestOverallApproaches(t *testing.T) {
	for k, f := range map[string]func(b *testing.B){
		"BenchmarkTestScan":            BenchmarkTestScan,
		"BenchmarkTestScanWithColumns": BenchmarkTestScanWithColumns,
		"BenchmarkTestScanNamed":       BenchmarkTestScanNamed,
	} {
		r := testing.Benchmark(f)
		t.Log(k, r.String(), r.MemString())
	}
}

func TestOverallSliceApproaches(t *testing.T) {
	ints := []int{2, 5, 10, 20, 50, 100}
	for _, testSize = range ints {
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

func BenchmarkTestSliceIncrement(b *testing.B) {
	slice := make([]*column, testSize)
	for j := 0; j < testSize; j++ {
		slice[j] = &column{}
	}
	var cnt int
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
	slice := make([]*column, testSize)
	for j := 0; j < testSize; j++ {
		slice[j] = &column{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < testSize; i++ {
			col := slice[i]
			col.name = "test1"
			col.typeID = 1
		}
	}
}

func BenchmarkTestDoubleIndex(b *testing.B) {
	slice := make([]*column, testSize)
	for j := 0; j < testSize; j++ {
		slice[j] = &column{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < testSize; i++ {
			slice[i].name = "test2"
			slice[i].typeID = 1
		}
	}
}
