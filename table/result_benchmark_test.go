package table

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/internal/result"
	"testing"
)

type series struct {
	id    uint64
	title string
	count uint32
}

func (s *series) UnmarshalYDB(res ydb.RawScanner) error {
	res.SeekItem("series_id")
	s.id = res.OUint64()
	res.SeekItem("title")
	s.title = res.OUTF8()
	res.SeekItem("release_date")
	s.count = res.ODatetime()
	return nil
}

func BenchmarkTestScanRow(b *testing.B) {
	b.ReportAllocs()
	r := result.PrepareScannerPerformanceTest(b.N)
	res := Result{Scanner: *r}
	s := series{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res.NextRow() {
			_ = res.ScanRaw(&s)
		}
	}
}

func (s *series) Scan(res *Result) error {
	res.SeekItem("series_id")
	s.id = res.OUint64()
	res.SeekItem("title")
	s.title = res.OUTF8()
	res.SeekItem("release_date")
	s.count = res.ODatetime()
	return nil
}

func BenchmarkTestDeprecatedScanStruct(b *testing.B) {
	b.ReportAllocs()
	r := result.PrepareScannerPerformanceTest(b.N)
	res := Result{Scanner: *r}
	s := series{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if res.NextRow() {
			s.Scan(&res)
		}
	}
}
