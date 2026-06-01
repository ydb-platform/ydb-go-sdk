package scanner

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/decimal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"google.golang.org/protobuf/proto"
)

func TestFieldName(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   any
		out  string
	}{
		{
			name: xtest.CurrentFileLine(),
			in: struct {
				Col0 string
			}{},
			out: "Col0",
		},
		{
			name: xtest.CurrentFileLine(),
			in: struct {
				Col0 string `sql:"col0"`
			}{},
			out: "col0",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.out, fieldName(reflect.ValueOf(tt.in).Type().Field(0), "sql"))
		})
	}
}

func TestStruct(t *testing.T) {
	newScannerData := func(mapping map[*Ydb.Column]*Ydb.Value) *Data {
		data := &Data{
			columns: make([]*Ydb.Column, 0, len(mapping)),
			values:  make([]*Ydb.Value, 0, len(mapping)),
		}
		for c, v := range mapping {
			data.columns = append(data.columns, c)
			data.values = append(data.values, v)
		}

		return data
	}

	type scanData struct {
		Utf8String    string
		Utf8Bytes     []byte
		StringString  string
		StringBytes   []byte
		Uint64Uint64  uint64
		Int64Int64    int64
		Uint32Uint64  uint64
		Uint32Int64   int64
		Uint32Uint32  uint32
		Int32Int64    int64
		Int32Int32    int32
		Uint16Uint64  uint64
		Uint16Int64   int64
		Uint16Uint32  uint32
		Uint16Int32   int32
		Uint16Uint16  uint16
		Int16Int64    int64
		Int16Int32    int32
		Uint8Uint64   uint64
		Uint8Int64    int64
		Uint8Uint32   uint32
		Uint8Int32    int32
		Uint8Uint16   uint16
		Int8Int64     int64
		Int8Int32     int32
		Int8Int16     int16
		BoolBool      bool
		DateTime      time.Time
		DatetimeTime  time.Time
		TimestampTime time.Time
	}
	var dst scanData
	err := Struct(newScannerData(map[*Ydb.Column]*Ydb.Value{
		Ydb.Column_builder{
			Name: "Utf8String",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UTF8.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			TextValue: proto.String("A"),
		}.Build(),
		Ydb.Column_builder{
			Name: "Utf8Bytes",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UTF8.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			TextValue: proto.String("A"),
		}.Build(),
		Ydb.Column_builder{
			Name: "StringString",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_STRING.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			BytesValue: []byte("A"),
		}.Build(),
		Ydb.Column_builder{
			Name: "StringBytes",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_STRING.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			BytesValue: []byte("A"),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint64Uint64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT64.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint64Value: proto.Uint64(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int64Int64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT64.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int64Value: proto.Int64(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint32Uint64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT32.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint32Int64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT32.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint32Uint32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT32.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int32Int64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT32.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int32Value: proto.Int32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int32Int32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT32.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int32Value: proto.Int32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint16Uint64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint16Int64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint16Uint32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint16Int32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint16Uint16",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int16Int64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int32Value: proto.Int32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int16Int32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int32Value: proto.Int32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint8Uint64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint8Int64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint8Uint32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint8Int32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Uint8Uint16",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_UINT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int8Int64",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int32Value: proto.Int32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int8Int32",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int32Value: proto.Int32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "Int8Int16",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_INT16.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Int32Value: proto.Int32(123),
		}.Build(),
		Ydb.Column_builder{
			Name: "BoolBool",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_BOOL.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			BoolValue: proto.Bool(true),
		}.Build(),
		Ydb.Column_builder{
			Name: "DateTime",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_DATE.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(100500),
		}.Build(),
		Ydb.Column_builder{
			Name: "DatetimeTime",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_DATETIME.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint32Value: proto.Uint32(100500),
		}.Build(),
		Ydb.Column_builder{
			Name: "TimestampTime",
			Type: Ydb.Type_builder{
				TypeId: Ydb.Type_TIMESTAMP.Enum(),
			}.Build(),
		}.Build(): Ydb.Value_builder{
			Uint64Value: proto.Uint64(12345678987654321),
		}.Build(),
	})).ScanStruct(&dst)
	require.NoError(t, err)
	require.Equal(t, scanData{
		Utf8String:    "A",
		Utf8Bytes:     []byte("A"),
		StringString:  "A",
		StringBytes:   []byte("A"),
		Uint64Uint64:  123,
		Int64Int64:    123,
		Uint32Uint64:  123,
		Uint32Int64:   123,
		Uint32Uint32:  123,
		Int32Int64:    123,
		Int32Int32:    123,
		Uint16Uint64:  123,
		Uint16Int64:   123,
		Uint16Uint32:  123,
		Uint16Int32:   123,
		Uint16Uint16:  123,
		Int16Int64:    123,
		Int16Int32:    123,
		Uint8Uint64:   123,
		Uint8Int64:    123,
		Uint8Uint32:   123,
		Uint8Int32:    123,
		Uint8Uint16:   123,
		Int8Int64:     123,
		Int8Int32:     123,
		Int8Int16:     123,
		BoolBool:      true,
		DateTime:      time.Unix(8683200000, 0).UTC(),
		DatetimeTime:  time.Unix(100500, 0),
		TimestampTime: time.Unix(12345678987, 654321000),
	}, dst)
}

func TestStructNotAPointer(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "a",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row struct {
		B string
		C string
	}
	err := scanner.ScanStruct(row)
	require.ErrorIs(t, err, errDstTypeIsNotAPointer)
}

func TestStructNotAPointerToStruct(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "a",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row string
	err := scanner.ScanStruct(&row)
	require.ErrorIs(t, err, errDstTypeIsNotAPointerToStruct)
}

func TestStructCastFailed(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row struct {
		A uint64
	}
	err := scanner.ScanStruct(&row)
	require.ErrorIs(t, err, value.ErrCannotCast)
}

func TestStructCastFailedErrMsg(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row struct {
		A uint64
	}
	err := scanner.ScanStruct(&row)
	require.ErrorContains(t, err, "scan error on struct field name 'A': cast failed")
}

func TestStructNotFoundColumns(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "a",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row struct {
		B string
		C string
	}
	err := scanner.ScanStruct(&row)
	require.ErrorIs(t, err, ErrColumnsNotFoundInRow)
}

func TestStructSkippedColumns(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test-a"),
			}.Build(),
		},
	))

	var row struct {
		A string
		C string `sql:"-"`
	}
	err := scanner.ScanStruct(&row)
	require.NoError(t, err)
	require.Equal(t, "test-a", row.A)
	require.Empty(t, row.C)
}

func TestStructWithAllowMissingColumnsFromSelect(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row struct {
		A string
		B string
		C string
	}
	err := scanner.ScanStruct(&row,
		WithAllowMissingColumnsFromSelect(),
	)
	require.NoError(t, err)
	require.Equal(t, "test", row.A)
	require.Equal(t, "", row.B)
	require.Equal(t, "", row.C)
}

func TestStructNotFoundFields(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "B",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "C",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row struct {
		A string
	}
	err := scanner.ScanStruct(&row)
	require.ErrorIs(t, err, ErrFieldsNotFoundInStruct)
}

func TestStructWithAllowMissingFieldsInStruct(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "B",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "C",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("test"),
			}.Build(),
		},
	))
	var row struct {
		A string
	}
	err := scanner.ScanStruct(&row,
		WithAllowMissingFieldsInStruct(),
	)
	require.NoError(t, err)
	require.Equal(t, "test", row.A)
}

func TestStructWithTagName(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "B",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "C",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("AA"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("BB"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("CC"),
			}.Build(),
		},
	))
	var row struct {
		A string `test:"A"`
		B string `test:"B"`
		C string `test:"C"`
	}
	err := scanner.ScanStruct(&row,
		WithTagName("test"),
	)
	require.NoError(t, err)
	require.Equal(t, "AA", row.A)
	require.Equal(t, "BB", row.B)
	require.Equal(t, "CC", row.C)
}

func TestScannerStructOrdering(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "B",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
			Ydb.Column_builder{
				Name: "C",
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				TextValue: proto.String("B"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("A"),
			}.Build(),
			Ydb.Value_builder{
				TextValue: proto.String("C"),
			}.Build(),
		},
	))
	var row struct {
		A string
		B string
		C string
	}
	err := scanner.ScanStruct(&row)
	require.NoError(t, err)
	require.Equal(t, "A", row.A)
	require.Equal(t, "B", row.B)
	require.Equal(t, "C", row.C)
}

func TestScannerDecimal(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					DecimalType: Ydb.DecimalType_builder{Scale: 9, Precision: 22}.Build(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				Low_128: proto.Uint64(10200000000),
			}.Build(),
		},
	))
	var row struct {
		A types.Decimal
	}
	expected := types.Decimal{Bytes: decimal.BigIntToByte(big.NewInt(10200000000), 22), Precision: 22, Scale: 9}
	err := scanner.ScanStruct(&row)
	require.NoError(t, err)
	require.Equal(t, expected, row.A)
}

func TestScannerDecimalNegative(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					DecimalType: Ydb.DecimalType_builder{Scale: 9, Precision: 22}.Build(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				Low_128:  proto.Uint64(18446744071704551616),
				High_128: 0xffffffffffffffff,
			}.Build(),
		},
	))
	var row struct {
		A types.Decimal
	}
	expected := types.Decimal{Bytes: decimal.BigIntToByte(big.NewInt(-2005000000), 22), Precision: 22, Scale: 9}
	err := scanner.ScanStruct(&row)
	require.NoError(t, err)
	require.Equal(t, expected, row.A)
}

func TestScannerDecimalBigDecimal(t *testing.T) {
	scanner := Struct(NewData(
		[]*Ydb.Column{
			Ydb.Column_builder{
				Name: "A",
				Type: Ydb.Type_builder{
					DecimalType: Ydb.DecimalType_builder{Scale: 9, Precision: 22}.Build(),
				}.Build(),
			}.Build(),
		},
		[]*Ydb.Value{
			Ydb.Value_builder{
				// val: 1844674407370955.1615
				Low_128:  proto.Uint64(3136633892082024448),
				High_128: 5421010862427522,
			}.Build(),
		},
	))
	var row struct {
		A types.Decimal
	}
	expectedVal := decimal.Decimal{
		Bytes:     [16]byte{0, 19, 66, 97, 114, 199, 77, 130, 43, 135, 143, 232, 0, 0, 0, 0},
		Precision: 22, Scale: 9,
	}
	err := scanner.ScanStruct(&row)
	require.NoError(t, err)
	require.Equal(t, expectedVal, row.A)
}
