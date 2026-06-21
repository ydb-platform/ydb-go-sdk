//go:build integration
// +build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
)

func buildOpaqueResultSet(columns, rows int) *Ydb.ResultSet {
	cols := make([]*Ydb.Column, 0, columns)
	for i := 0; i < columns; i++ {
		cols = append(cols, Ydb.Column_builder{
			Name: fmt.Sprintf("col_%d", i),
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UTF8.Enum(),
			}.Build(),
		}.Build())
	}

	rs := make([]*Ydb.Value, 0, rows)
	for r := 0; r < rows; r++ {
		items := make([]*Ydb.Value, 0, columns)
		for c := 0; c < columns; c++ {
			items = append(items, Ydb.Value_builder{
				TextValue: proto.String(fmt.Sprintf("value_%d_%d", r, c)),
			}.Build())
		}
		rs = append(rs, Ydb.Value_builder{Items: items}.Build())
	}

	return Ydb.ResultSet_builder{
		Columns: cols,
		Rows:    rs,
	}.Build()
}

func BenchmarkResultSet_FullRead(b *testing.B) {
	rs := buildOpaqueResultSet(100, 1000)
	data, _ := proto.Marshal(rs)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		target := &Ydb.ResultSet{}
		_ = proto.Unmarshal(data, target)
		for _, row := range target.GetRows() {
			for _, item := range row.GetItems() {
				_ = item.GetTextValue()
			}
		}
	}
}

func BenchmarkResultSet_PartialRead(b *testing.B) {
	rs := buildOpaqueResultSet(100, 1000)
	data, _ := proto.Marshal(rs)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		target := &Ydb.ResultSet{}
		_ = proto.Unmarshal(data, target)
		for _, row := range target.GetRows() {
			if len(row.GetItems()) > 0 {
				_ = row.GetItems()[0].GetTextValue()
			}
		}
	}
}