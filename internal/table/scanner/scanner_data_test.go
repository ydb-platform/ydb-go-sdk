package scanner

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type column struct {
	name        string
	typeID      Ydb.Type_PrimitiveTypeId
	optional    bool
	scanner     bool
	ydbvalue    bool
	testDefault bool
	nilValue    bool
}

type intIncScanner int64

func (s *intIncScanner) Scan(src interface{}) error {
	v, ok := src.(int64)
	if !ok {
		return fmt.Errorf("wrong type: %T, exp: int64", src)
	}
	*s = intIncScanner(v + 10)

	return nil
}

type dateScanner time.Time

func (s *dateScanner) Scan(src interface{}) error {
	v, ok := src.(time.Time)
	if !ok {
		return fmt.Errorf("wrong type: %T, exp: time.Time", src)
	}
	*s = dateScanner(v)

	return nil
}

var scannerData = []struct {
	name             string
	count            int
	columns          []*column
	values           []indexed.RequiredOrOptional
	setColumns       []string
	setColumnIndexes []int
}{
	{
		name:  "Scan UUID, DATE",
		count: 10,
		columns: []*column{{
			name:   "uuid",
			typeID: Ydb.Type_UUID,
		}, {
			name:   "date",
			typeID: Ydb.Type_DATE,
		}},
		values: []indexed.RequiredOrOptional{new([16]byte), new(time.Time)},
	},
	{
		name:  "Scan JSON, DOUBLE",
		count: 20,
		columns: []*column{{
			name:   "json",
			typeID: Ydb.Type_JSON,
		}, {
			name:   "double",
			typeID: Ydb.Type_DOUBLE,
		}},
		values: []indexed.RequiredOrOptional{new([]byte), new(float64)},
	},
	{
		name:  "Scan INT8, INT16, INT32",
		count: 210,
		columns: []*column{{
			name:   "int8",
			typeID: Ydb.Type_INT8,
		}, {
			name:   "int16",
			typeID: Ydb.Type_INT16,
		}, {
			name:   "int32",
			typeID: Ydb.Type_INT32,
		}},
		values:     []indexed.RequiredOrOptional{new(int8), new(int16), new(int32)},
		setColumns: []string{"int8", "int16", "int32"},
	},
	{
		name:  "Scan YSON, DOUBLE. Zero rows in the result",
		count: 0,
		columns: []*column{{
			name:   "yson",
			typeID: Ydb.Type_YSON,
		}, {
			name:   "double",
			typeID: Ydb.Type_DOUBLE,
		}},
		values: []indexed.RequiredOrOptional{new([]byte), new(float64)},
	},
	{
		name:  "Scan JSON, FLOAT",
		count: 1000,
		columns: []*column{{
			name:   "jsondocument",
			typeID: Ydb.Type_JSON_DOCUMENT,
		}, {
			name:   "float",
			typeID: Ydb.Type_FLOAT,
		}},
		values: []indexed.RequiredOrOptional{new([]byte), new(float32)},
	},
	{
		name:  "Scan UINT8, UINT16, UINT32",
		count: 200,
		columns: []*column{{
			name:   "uint8",
			typeID: Ydb.Type_UINT8,
		}, {
			name:   "uint16",
			typeID: Ydb.Type_UINT16,
		}, {
			name:   "uint32",
			typeID: Ydb.Type_UINT32,
		}},
		values: []indexed.RequiredOrOptional{new(uint8), new(uint16), new(uint32)},
	},
	{
		name:  "Scan DYNUMBER, Type_UTF8, Type_STRING",
		count: 5,
		columns: []*column{{
			name:   "dynumber",
			typeID: Ydb.Type_DYNUMBER,
		}, {
			name:   "utf8",
			typeID: Ydb.Type_UTF8,
		}, {
			name:   "string",
			typeID: Ydb.Type_STRING,
		}},
		values: []indexed.RequiredOrOptional{new(string), new(string), new([]byte)},
	},
	{
		name:  "Scan float32, int64, uint64 and skip other columns",
		count: 15,
		columns: []*column{{
			name:   "float32",
			typeID: Ydb.Type_FLOAT,
		}, {
			name:   "utf8",
			typeID: Ydb.Type_UTF8,
		}, {
			name:   "int64",
			typeID: Ydb.Type_INT64,
		}, {
			name:   "string",
			typeID: Ydb.Type_STRING,
		}, {
			name:   "uint64",
			typeID: Ydb.Type_UINT64,
		}},
		values:           []indexed.RequiredOrOptional{new(float32), new(int64), new(uint64)},
		setColumns:       []string{"float32", "int64", "uint64"},
		setColumnIndexes: []int{0, 2, 4},
	},
	{
		name:  "Scan TIMESTAMP, BOOL, INTERVAL in a different order",
		count: 20,
		columns: []*column{{
			name:   "timestamp",
			typeID: Ydb.Type_TIMESTAMP,
		}, {
			name:   "bool",
			typeID: Ydb.Type_BOOL,
		}, {
			name:   "interval",
			typeID: Ydb.Type_INTERVAL,
		}},
		values:           []indexed.RequiredOrOptional{new(bool), new(time.Duration), new(time.Time)},
		setColumns:       []string{"bool", "interval", "timestamp"},
		setColumnIndexes: []int{1, 2, 0},
	},
	{
		name:  "ScanWithDefaults for required columns TZ_TIMESTAMP, TZ_DATE, TZ_DATETIME in a different order",
		count: 300,
		columns: []*column{{
			name:        "tztimestamp",
			typeID:      Ydb.Type_TZ_TIMESTAMP,
			testDefault: true,
		}, {
			name:        "tzdate",
			typeID:      Ydb.Type_TZ_DATE,
			testDefault: true,
		}, {
			name:        "tzdatetime",
			typeID:      Ydb.Type_TZ_DATETIME,
			testDefault: true,
		}},
		values:           []indexed.RequiredOrOptional{new(time.Time), new(time.Time), new(time.Time)},
		setColumns:       []string{"tztimestamp", "tzdatetime", "tzdate"},
		setColumnIndexes: []int{0, 2, 1},
	},
	{
		name:  "Scan int64, float, json as ydb.valueType",
		count: 100,
		columns: []*column{{
			name:     "valueint64",
			typeID:   Ydb.Type_INT64,
			ydbvalue: true,
		}, {
			name:     "valuefloat",
			typeID:   Ydb.Type_FLOAT,
			ydbvalue: true,
		}, {
			name:     "valuejson",
			typeID:   Ydb.Type_JSON,
			ydbvalue: true,
		}},
		values: []indexed.RequiredOrOptional{
			new(types.Value),
			new(types.Value),
			new(types.Value),
		},
	},
	{
		name:  "Scan table with single column",
		count: 10,
		columns: []*column{{
			name:   "datetime",
			typeID: Ydb.Type_DATETIME,
		}},
		values: []indexed.RequiredOrOptional{new(time.Time)},
	},
	{
		name:  "Scan optional values",
		count: 500,
		columns: []*column{{
			name:     "otzdatetime",
			typeID:   Ydb.Type_TZ_DATETIME,
			optional: true,
		}, {
			name:     "ouint16",
			typeID:   Ydb.Type_UINT16,
			optional: true,
		}, {
			name:     "ostring",
			typeID:   Ydb.Type_STRING,
			optional: true,
		}},
		values: []indexed.RequiredOrOptional{new(*time.Time), new(*uint16), new(*[]byte)},
	},
	{
		name:  "Scan optional values",
		count: 30,
		columns: []*column{{
			name:     "ointerval",
			typeID:   Ydb.Type_INTERVAL,
			optional: true,
		}, {
			name:     "ouuid",
			typeID:   Ydb.Type_UUID,
			optional: true,
		}, {
			name:     "odouble",
			typeID:   Ydb.Type_DOUBLE,
			optional: true,
		}},
		values: []indexed.RequiredOrOptional{new(*time.Duration), new(*[16]byte), new(*float64)},
	},
	{
		name:  "Scan int64, date, string as ydb.Scanner",
		count: 4,
		columns: []*column{{
			name:    "sint64",
			typeID:  Ydb.Type_INT64,
			scanner: true,
		}, {
			name:    "sdate",
			typeID:  Ydb.Type_DATE,
			scanner: true,
		}, {
			name:    "sstring",
			typeID:  Ydb.Type_STRING,
			scanner: true,
		}},
		values: []indexed.RequiredOrOptional{new(intIncScanner), new(dateScanner), new([]byte)},
	},
	{
		name:  "Scan optional int64, date, string as ydb.Scanner",
		count: 30,
		columns: []*column{{
			name:     "sint64",
			typeID:   Ydb.Type_INT64,
			optional: true,
			scanner:  true,
		}, {
			name:     "sdate",
			typeID:   Ydb.Type_DATE,
			optional: true,
			scanner:  true,
		}, {
			name:     "sstring",
			typeID:   Ydb.Type_STRING,
			optional: true,
		}},
		values: []indexed.RequiredOrOptional{new(intIncScanner), new(dateScanner), new(*[]byte)},
	},
	{
		name:  "ScanWithDefaults optional int64, date, string with null values as ydb.Scanner",
		count: 30,
		columns: []*column{{
			name:     "sint64",
			typeID:   Ydb.Type_INT64,
			optional: true,
			scanner:  true,
		}, {
			name:     "sdate",
			typeID:   Ydb.Type_DATE,
			optional: true,
			scanner:  true,
		}, {
			name:     "sstring",
			typeID:   Ydb.Type_STRING,
			optional: true,
			scanner:  true,
			nilValue: true,
		}},
		values: []indexed.RequiredOrOptional{new(intIncScanner), new(dateScanner), new(*[]byte)},
	},
	{
		name:  "ScanWithDefaults optional int32, time interval, string",
		count: 30,
		columns: []*column{{
			name:        "oint32",
			typeID:      Ydb.Type_INT32,
			optional:    true,
			testDefault: true,
		}, {
			name:        "otimeinterval",
			typeID:      Ydb.Type_INTERVAL,
			optional:    true,
			testDefault: true,
		}, {
			name:        "ostring",
			typeID:      Ydb.Type_STRING,
			optional:    true,
			testDefault: true,
		}},
		values: []indexed.RequiredOrOptional{new(int32), new(time.Duration), new([]byte)},
	},
	{
		name:  "ScanWithDefaults optional int32, time interval, string, nil values applied as default value types",
		count: 14,
		columns: []*column{{
			name:        "oint32",
			typeID:      Ydb.Type_INT32,
			optional:    true,
			testDefault: true,
			nilValue:    true,
		}, {
			name:        "otimeinterval",
			typeID:      Ydb.Type_INTERVAL,
			optional:    true,
			testDefault: true,
			nilValue:    true,
		}, {
			name:        "ostring",
			typeID:      Ydb.Type_STRING,
			optional:    true,
			testDefault: true,
			nilValue:    true,
		}},
		values: []indexed.RequiredOrOptional{new(int32), new(time.Duration), new([]byte)},
	},
	{
		name:  "Scan optional int32, time interval, string. All values are null",
		count: 15,
		columns: []*column{{
			name:     "oint32",
			typeID:   Ydb.Type_INT32,
			optional: true,
			nilValue: true,
		}, {
			name:     "otimeinterval",
			typeID:   Ydb.Type_INTERVAL,
			optional: true,
			nilValue: true,
		}, {
			name:     "ostring",
			typeID:   Ydb.Type_STRING,
			optional: true,
			nilValue: true,
		}},
		values: []indexed.RequiredOrOptional{new(*int32), new(*time.Duration), new(*[]byte)},
	},
	{
		name:  "Scan optional uint8, yson, tzdatetime, uuid. All values are null",
		count: 15,
		columns: []*column{{
			name:     "ouint8",
			typeID:   Ydb.Type_UINT8,
			optional: true,
			nilValue: true,
		}, {
			name:     "oyson",
			typeID:   Ydb.Type_YSON,
			optional: true,
			nilValue: true,
		}, {
			name:     "otzdatetime",
			typeID:   Ydb.Type_TZ_DATETIME,
			optional: true,
			nilValue: true,
		}, {
			name:     "ouuid",
			typeID:   Ydb.Type_UUID,
			optional: true,
			nilValue: true,
		}},
		values: []indexed.RequiredOrOptional{new(*uint8), new(*[]byte), new(*time.Time), new(*[16]byte)},
	},
	{
		name:  "Scan string as byte array.",
		count: 19,
		columns: []*column{{
			name:   "string",
			typeID: Ydb.Type_STRING,
		}},
		values: []indexed.RequiredOrOptional{new([]byte)},
	},
	{
		name:  "Scan optional string as byte array.",
		count: 18,
		columns: []*column{{
			name:     "string",
			typeID:   Ydb.Type_STRING,
			optional: true,
		}},
		values: []indexed.RequiredOrOptional{new(*[]byte)},
	},
	{
		name:  "Scan optional null string as byte array.",
		count: 17,
		columns: []*column{{
			name:     "string",
			typeID:   Ydb.Type_STRING,
			optional: true,
			nilValue: true,
		}},
		values: []indexed.RequiredOrOptional{new(*[]byte)},
	},
	{
		name:  "Scan optional default string as byte array.",
		count: 16,
		columns: []*column{{
			name:        "string",
			typeID:      Ydb.Type_STRING,
			optional:    true,
			nilValue:    true,
			testDefault: true,
		}},
		values: []indexed.RequiredOrOptional{new([]byte)},
	},
}

func initScanner() *valueScanner {
	res := valueScanner{
		set: &Ydb.ResultSet{
			Columns:   nil,
			Rows:      nil,
			Truncated: false,
		},
		row: nil,
		stack: scanStack{
			v: nil,
			p: 0,
		},
		nextRow:       0,
		nextItem:      0,
		columnIndexes: nil,
		err:           nil,
	}

	return &res
}

func PrepareScannerPerformanceTest(count int) *valueScanner {
	res := initScanner()
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
	res.set.Rows = []*Ydb.Value{}
	for i := 0; i < count; i++ {
		res.set.Rows = append(res.set.GetRows(), &Ydb.Value{
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
	res.converter = &rawConverter{res}

	return res
}
