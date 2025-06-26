package scanner

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestFieldName(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   interface{}
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
	newScannerData := func(mapping map[*Ydb.Column]*Ydb.Value) *data {
		data := &data{
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
		{
			Name: "Utf8String",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UTF8,
				},
			},
		}: {
			Value: &Ydb.Value_TextValue{
				TextValue: "A",
			},
		},
		{
			Name: "Utf8Bytes",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UTF8,
				},
			},
		}: {
			Value: &Ydb.Value_TextValue{
				TextValue: "A",
			},
		},
		{
			Name: "StringString",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_STRING,
				},
			},
		}: {
			Value: &Ydb.Value_BytesValue{
				BytesValue: []byte("A"),
			},
		},
		{
			Name: "StringBytes",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_STRING,
				},
			},
		}: {
			Value: &Ydb.Value_BytesValue{
				BytesValue: []byte("A"),
			},
		},
		{
			Name: "Uint64Uint64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT64,
				},
			},
		}: {
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: 123,
			},
		},
		{
			Name: "Int64Int64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT64,
				},
			},
		}: {
			Value: &Ydb.Value_Int64Value{
				Int64Value: 123,
			},
		},
		{
			Name: "Uint32Uint64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT32,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint32Int64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT32,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint32Uint32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT32,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Int32Int64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT32,
				},
			},
		}: {
			Value: &Ydb.Value_Int32Value{
				Int32Value: 123,
			},
		},
		{
			Name: "Int32Int32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT32,
				},
			},
		}: {
			Value: &Ydb.Value_Int32Value{
				Int32Value: 123,
			},
		},
		{
			Name: "Uint16Uint64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint16Int64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint16Uint32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint16Int32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint16Uint16",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Int16Int64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT16,
				},
			},
		}: {
			Value: &Ydb.Value_Int32Value{
				Int32Value: 123,
			},
		},
		{
			Name: "Int16Int32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT16,
				},
			},
		}: {
			Value: &Ydb.Value_Int32Value{
				Int32Value: 123,
			},
		},
		{
			Name: "Uint8Uint64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint8Int64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint8Uint32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint8Int32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Uint8Uint16",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT16,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 123,
			},
		},
		{
			Name: "Int8Int64",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT16,
				},
			},
		}: {
			Value: &Ydb.Value_Int32Value{
				Int32Value: 123,
			},
		},
		{
			Name: "Int8Int32",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT16,
				},
			},
		}: {
			Value: &Ydb.Value_Int32Value{
				Int32Value: 123,
			},
		},
		{
			Name: "Int8Int16",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_INT16,
				},
			},
		}: {
			Value: &Ydb.Value_Int32Value{
				Int32Value: 123,
			},
		},
		{
			Name: "BoolBool",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_BOOL,
				},
			},
		}: {
			Value: &Ydb.Value_BoolValue{
				BoolValue: true,
			},
		},
		{
			Name: "DateTime",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_DATE,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 100500,
			},
		},
		{
			Name: "DatetimeTime",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_DATETIME,
				},
			},
		}: {
			Value: &Ydb.Value_Uint32Value{
				Uint32Value: 100500,
			},
		},
		{
			Name: "TimestampTime",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_TIMESTAMP,
				},
			},
		}: {
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: 12345678987654321,
			},
		},
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
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "a",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
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
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "a",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
		},
	))
	var row string
	err := scanner.ScanStruct(&row)
	require.ErrorIs(t, err, errDstTypeIsNotAPointerToStruct)
}

func TestStructCastFailed(t *testing.T) {
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
		},
	))
	var row struct {
		A uint64
	}
	err := scanner.ScanStruct(&row)
	require.ErrorIs(t, err, value.ErrCannotCast)
}

func TestStructCastFailedErrMsg(t *testing.T) {
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
		},
	))
	var row struct {
		A uint64
	}
	err := scanner.ScanStruct(&row)
	require.ErrorContains(t, err, "scan error on struct field name 'A': cast failed")
}

func TestStructNotFoundColumns(t *testing.T) {
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "a",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
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
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test-a",
				},
			},
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
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
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
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "B",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "C",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
		},
	))
	var row struct {
		A string
	}
	err := scanner.ScanStruct(&row)
	require.ErrorIs(t, err, ErrFieldsNotFoundInStruct)
}

func TestStructWithAllowMissingFieldsInStruct(t *testing.T) {
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "B",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "C",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
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
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "B",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "C",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "AA",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "BB",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "CC",
				},
			},
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
	scanner := Struct(Data(
		[]*Ydb.Column{
			{
				Name: "B",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "A",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "C",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "B",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "A",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "C",
				},
			},
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
