package query

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var benchmarkRowChecksum uint64

func BenchmarkQueryRows(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		frame := benchmarkRowsFrame(b, count)
		for _, mode := range []string{"decode", "scan", "named", "struct", "values"} {
			for _, decoder := range []string{"protobuf", "wire"} {
				b.Run(fmt.Sprintf("rows=%d/%s/%s", count, mode, decoder), func(b *testing.B) {
					b.SetBytes(int64(len(frame)))
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						rows := benchmarkDecodeRows(b, frame, decoder, mode)
						var hash uint64
						for _, row := range rows {
							id, err := benchmarkReadRow(row, mode)
							if err != nil {
								b.Fatal(err)
							}
							hash += id
						}
						if mode != "decode" && hash != uint64(count*(count-1)/2) {
							b.Fatalf("checksum = %d, want %d", hash, count*(count-1)/2)
						}
						benchmarkRowChecksum = hash
					}
				})
			}
		}
	}
}

func BenchmarkQueryWideRows(b *testing.B) {
	for _, width := range []int{16, 128, 1024} {
		frame := benchmarkWideRowsFrame(b, width)
		for _, mode := range []string{"scan", "named", "struct", "values"} {
			for _, decoder := range []string{"protobuf", "wire"} {
				b.Run(fmt.Sprintf("columns=%d/%s/%s", width, mode, decoder), func(b *testing.B) {
					read := benchmarkWideRowReader(width, mode)
					want := uint64(width * (width + 1) / 2)
					b.SetBytes(int64(len(frame)))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						row := benchmarkDecodeRows(b, frame, decoder, mode)[0]
						got, err := read(row)
						if err != nil {
							b.Fatal(err)
						}
						if got != want {
							b.Fatalf("checksum = %d, want %d", got, want)
						}
						benchmarkRowChecksum = got
					}
				})
			}
		}
	}
}

func benchmarkWideRowsFrame(b *testing.B, width int) []byte {
	b.Helper()
	columns := make([]*Ydb.Column, width)
	items := make([]*Ydb.Value, width)
	for column := range columns {
		columns[column] = &Ydb.Column{
			Name: fmt.Sprintf("column_%d", column),
			Type: wirePrimitive(Ydb.Type_UINT64),
		}
		items[column] = &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: uint64(column + 1)}}
	}
	frame, err := proto.Marshal(&Ydb_Query.ExecuteQueryResponsePart{ResultSet: &Ydb.ResultSet{
		Columns: columns,
		Rows:    []*Ydb.Value{{Items: items}},
	}})
	if err != nil {
		b.Fatal(err)
	}

	return frame
}

func benchmarkWideRowReader(width int, mode string) func(query.Row) (uint64, error) {
	values := make([]uint64, width)
	refs := make([]any, width)
	named := make([]scanner.NamedDestination, width)
	fields := make([]reflect.StructField, width)
	for column := range values {
		name := fmt.Sprintf("column_%d", column)
		refs[column] = &values[column]
		named[width-column-1] = scanner.NamedRef(name, &values[column])
		fields[width-column-1] = reflect.StructField{
			Name: fmt.Sprintf("Column%d", column),
			Type: reflect.TypeFor[uint64](),
			Tag:  reflect.StructTag(fmt.Sprintf(`sql:"%s"`, name)),
		}
	}
	structDst := reflect.New(reflect.StructOf(fields))

	return func(row query.Row) (uint64, error) {
		var err error
		switch mode {
		case "scan":
			err = row.Scan(refs...)
		case "named":
			err = row.ScanNamed(named...)
		case "struct":
			err = row.ScanStruct(structDst.Interface())
			for column := range values {
				values[column] = structDst.Elem().Field(width - column - 1).Uint()
			}
		case "values":
			for column, v := range row.Values() {
				if err = value.CastTo(v, &values[column]); err != nil {
					return 0, err
				}
			}
		}
		var checksum uint64
		for _, v := range values {
			checksum += v
		}

		return checksum, err
	}
}

func benchmarkDecodeRows(b *testing.B, frame []byte, decoder, mode string) []query.Row {
	b.Helper()
	if decoder == "wire" {
		part, err := decodeWirePart(frame)
		if err != nil {
			b.Fatal(err)
		}
		if mode == "decode" {
			return nil
		}
		rows := make([]query.Row, part.RowCount())
		for j := range rows {
			rows[j] = part.row(j, part.Meta().GetResultSet().GetColumns())
		}

		return rows
	}
	var part Ydb_Query.ExecuteQueryResponsePart
	if err := proto.Unmarshal(frame, &part); err != nil {
		b.Fatal(err)
	}
	if mode == "decode" {
		return nil
	}
	rs := part.GetResultSet()
	rows := make([]query.Row, len(rs.GetRows()))
	for j, row := range rs.GetRows() {
		rows[j] = NewRow(rs.GetColumns(), row)
	}

	return rows
}

func benchmarkRowsFrame(b *testing.B, count int) []byte {
	b.Helper()
	part := &Ydb_Query.ExecuteQueryResponsePart{ResultSet: &Ydb.ResultSet{
		Columns: []*Ydb.Column{
			{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)},
			{Name: "count", Type: wirePrimitive(Ydb.Type_UINT32)},
		},
		Rows: make([]*Ydb.Value, count),
	}}
	for i := range part.GetResultSet().GetRows() {
		part.GetResultSet().Rows[i] = &Ydb.Value{Items: []*Ydb.Value{
			{Value: &Ydb.Value_Uint64Value{Uint64Value: uint64(i)}},
			{Value: &Ydb.Value_Uint32Value{Uint32Value: uint32(i)}},
		}}
	}
	frame, err := proto.Marshal(part)
	if err != nil {
		b.Fatal(err)
	}

	return frame
}

func benchmarkReadRow(row query.Row, mode string) (uint64, error) {
	var id uint64
	var count uint32
	switch mode {
	case "scan":
		err := row.Scan(&id, &count)

		return id, err
	case "named":
		err := row.ScanNamed(scanner.NamedRef("id", &id), scanner.NamedRef("count", &count))

		return id, err
	case "struct":
		var dst struct {
			ID    uint64 `sql:"id"`
			Count uint32 `sql:"count"`
		}
		err := row.ScanStruct(&dst)

		return dst.ID, err
	case "values":
		err := value.CastTo(row.Values()[0], &id)

		return id, err
	default:

		return 0, nil
	}
}
