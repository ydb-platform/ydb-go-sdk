package scanner

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func valueFromPrimitiveTypeID(c *column, r xrand.Rand) (*Ydb.Value, interface{}) {
	rv := r.Int64(math.MaxInt16)
	switch c.typeID {
	case Ydb.Type_BOOL:
		return getValueBool(c, rv)
	case Ydb.Type_INT8:
		return getValueInt8(c, rv)
	case Ydb.Type_UINT8:
		return getValueUint8(c, rv)
	case Ydb.Type_INT16:
		return getValueInt16(c, rv)
	case Ydb.Type_UINT16:
		return getValueUint16(c, rv)
	case Ydb.Type_INT32:
		return getValueInt32(c, rv)
	case Ydb.Type_UINT32:
		return getValueUint32(c, rv)
	case Ydb.Type_INT64:
		return getValueInt64(c, rv)
	case Ydb.Type_UINT64:
		return getValueUint64(c, rv)
	case Ydb.Type_FLOAT:
		return getValueFloat32(c, rv)
	case Ydb.Type_DOUBLE:
		return getValueFloat64(c, rv)
	case Ydb.Type_DATE:
		return getValueDate(c, rv)
	case Ydb.Type_DATETIME:
		return getValueDateTime(c, rv)
	case Ydb.Type_TIMESTAMP:
		return getValueTimestamp(c, rv)
	case Ydb.Type_INTERVAL:
		return getValueInterval(c, rv)
	case Ydb.Type_TZ_DATE:
		return getValueTzDate(c)
	case Ydb.Type_TZ_DATETIME:
		return getValueTzDatetime(c, rv)
	case Ydb.Type_TZ_TIMESTAMP:
		return getValueTzTimestamp(c, rv)
	case Ydb.Type_STRING:
		return getValueString(c, rv)
	case Ydb.Type_UTF8:
		return getValueUtf8(c, rv)
	case Ydb.Type_YSON:
		return getValueYSON(c, rv)
	case Ydb.Type_JSON:
		return getValueJSON(c, rv)
	case Ydb.Type_UUID:
		return getValueUUID(c, rv)
	case Ydb.Type_JSON_DOCUMENT:
		return getValueJSONDocument(c, rv)
	case Ydb.Type_DYNUMBER:
		return getValueDyNumber(c, rv)
	default:
		panic("ydb: unexpected types")
	}
}

// getValueDyNumber extracts a dynamic number value.
func getValueDyNumber(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := strconv.FormatUint(uint64(rv), 10)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueJSONDocument extracts a JSON document value.
func getValueJSONDocument(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := strconv.FormatUint(uint64(rv), 10)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	src := []byte(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueUUID extracts a UUID value.
func getValueUUID(c *column, rv int64) (*Ydb.Value, interface{}) {
	if c.nilValue {
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_NullFlagValue{},
		}
		if c.testDefault {
			var dv [16]byte

			return ydbval, &dv
		}
		var dv *[16]byte

		return ydbval, &dv
	}
	v := [16]byte{}
	binary.BigEndian.PutUint64(v[0:8], uint64(rv))
	binary.BigEndian.PutUint64(v[8:16], uint64(rv))
	ydbval := &Ydb.Value{
		High_128: binary.BigEndian.Uint64(v[0:8]),
		Value: &Ydb.Value_Low_128{
			Low_128: binary.BigEndian.Uint64(v[8:16]),
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueJSON extracts a JSON value.
func getValueJSON(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := strconv.FormatUint(uint64(rv), 10)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	if c.ydbvalue {
		vp := types.JSONValue(v)

		return ydbval, &vp
	}
	src := []byte(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueYSON extracts a YSON (Yandex Simple Object Notation) value.
func getValueYSON(c *column, rv int64) (*Ydb.Value, interface{}) {
	if c.nilValue {
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_NullFlagValue{},
		}
		if c.testDefault {
			var dv []byte

			return ydbval, &dv
		}
		var dv *[]byte

		return ydbval, &dv
	}
	v := strconv.FormatUint(uint64(rv), 10)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	src := []byte(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueUtf8 extracts an UTF-8 string value.
func getValueUtf8(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := strconv.FormatUint(uint64(rv), 10)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueString extracts a string value from an int64 value. If `c.nilValue` is true, it returns a null value.
func getValueString(c *column, rv int64) (*Ydb.Value, interface{}) {
	if c.nilValue {
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_NullFlagValue{},
		}
		if c.testDefault {
			var dv []byte

			return ydbval, &dv
		}
		var dv *[]byte

		return ydbval, &dv
	}
	v := make([]byte, 16)
	binary.BigEndian.PutUint64(v[0:8], uint64(rv))
	binary.BigEndian.PutUint64(v[8:16], uint64(rv))
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_BytesValue{
			BytesValue: v,
		},
	}
	src := v
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueTzTimestamp extracts the current timestamp in the format of "2006-01-02T15:04:05.000000,Europe/Berlin".
func getValueTzTimestamp(c *column, rv int64) (*Ydb.Value, interface{}) {
	rv %= time.Now().Unix()
	v := value.TimestampToTime(uint64(rv)).Format(value.LayoutTzTimestamp) + ",Europe/Berlin"
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	src, _ := value.TzTimestampToTime(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueTzDatetime extracts the current time in the format of "2006-01-02T15:04:05,Europe/Berlin".
func getValueTzDatetime(c *column, rv int64) (*Ydb.Value, interface{}) {
	if c.nilValue {
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_NullFlagValue{},
		}
		if c.testDefault {
			var dv time.Time

			return ydbval, &dv
		}
		var dv *time.Time

		return ydbval, &dv
	}
	rv %= time.Now().Unix()
	v := value.DatetimeToTime(uint32(rv)).Format(value.LayoutTzDatetime) + ",Europe/Berlin"
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	src, _ := value.TzDatetimeToTime(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueTzDate extracts the current time in the format of "2006-01-02,Europe/Berlin"
func getValueTzDate(c *column) (*Ydb.Value, interface{}) {
	v := time.Now().Format(value.LayoutDate) + ",Europe/Berlin"
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_TextValue{
			TextValue: v,
		},
	}
	src, _ := value.TzDateToTime(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueInterval extracts int64 value from the input int64 and returns it wrapped in a Ydb.Value struct.
func getValueInterval(c *column, rv int64) (*Ydb.Value, interface{}) {
	if c.nilValue {
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_NullFlagValue{},
		}
		if c.testDefault {
			var dv time.Duration

			return ydbval, &dv
		}
		var dv *time.Duration

		return ydbval, &dv
	}
	rv %= time.Now().Unix()
	v := rv
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Int64Value{
			Int64Value: v,
		},
	}
	src := value.IntervalToDuration(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueTimestamp extracts uint64 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueTimestamp(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := uint64(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Uint64Value{
			Uint64Value: v,
		},
	}
	src := value.TimestampToTime(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueDateTime extracts uint32 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueDateTime(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := uint32(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Uint32Value{
			Uint32Value: v,
		},
	}
	src := value.DatetimeToTime(v)
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueDate extracts uint32 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueDate(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := uint32(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Uint32Value{
			Uint32Value: v,
		},
	}
	src := value.DateToTime(v)
	if c.scanner {
		s := dateScanner(src)

		return ydbval, &s
	}
	if c.optional && !c.testDefault {
		vp := &src

		return ydbval, &vp
	}

	return ydbval, &src
}

// getValueFloat64 extracts float64 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueFloat64(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := float64(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_DoubleValue{
			DoubleValue: v,
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueFloat32 extracts float32 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueFloat32(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := float32(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_FloatValue{
			FloatValue: v,
		},
	}
	if c.ydbvalue {
		vp := types.FloatValue(v)

		return ydbval, &vp
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueUint64 extracts uint64 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueUint64(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := uint64(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Uint64Value{
			Uint64Value: v,
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueInt64 extracts int64 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueInt64(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := rv
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Int64Value{
			Int64Value: v,
		},
	}
	if c.ydbvalue {
		vp := types.Int64Value(v)

		return ydbval, &vp
	}
	if c.scanner {
		s := intIncScanner(v + 10)

		return ydbval, &s
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueUint32 extracts uint32 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueUint32(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := uint32(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Uint32Value{
			Uint32Value: v,
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueInt32 extracts an int32 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueInt32(c *column, rv int64) (*Ydb.Value, interface{}) {
	if c.nilValue {
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_NullFlagValue{},
		}
		if c.testDefault {
			var dv int32

			return ydbval, &dv
		}
		var dv *int32

		return ydbval, &dv
	}
	v := int32(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Int32Value{
			Int32Value: v,
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueUint16 extracts an uint16 value from an uint64 and returns it wrapped in a Ydb.Value struct.
func getValueUint16(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := uint16(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Uint32Value{
			Uint32Value: uint32(v),
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueInt16 extracts an int16 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueInt16(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := int16(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Int32Value{
			Int32Value: int32(v),
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueUint8 extracts an uint8 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueUint8(c *column, rv int64) (*Ydb.Value, interface{}) {
	if c.nilValue {
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_NullFlagValue{},
		}
		if c.testDefault {
			var dv uint8

			return ydbval, &dv
		}
		var dv *uint8

		return ydbval, &dv
	}
	v := uint8(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Uint32Value{
			Uint32Value: uint32(v),
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueInt8 extracts an int8 value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueInt8(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := int8(rv)
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_Int32Value{
			Int32Value: int32(v),
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

// getValueBool extracts a boolean value from an int64 and returns it wrapped in a Ydb.Value struct.
func getValueBool(c *column, rv int64) (*Ydb.Value, interface{}) {
	v := rv%2 == 1
	ydbval := &Ydb.Value{
		Value: &Ydb.Value_BoolValue{
			BoolValue: v,
		},
	}
	if c.optional && !c.testDefault {
		vp := &v

		return ydbval, &vp
	}

	return ydbval, &v
}

func getResultSet(count int, col []*column) (result *Ydb.ResultSet, testValues [][]indexed.RequiredOrOptional) {
	result = &Ydb.ResultSet{}
	for _, c := range col {
		t := &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: c.typeID,
			},
		}
		if c.optional {
			t = &Ydb.Type{
				Type: &Ydb.Type_OptionalType{
					OptionalType: &Ydb.OptionalType{
						Item: t,
					},
				},
			}
		}
		result.Columns = append(
			result.Columns,
			&Ydb.Column{
				Name: c.name,
				Type: t,
			},
		)
	}

	r := xrand.New(xrand.WithLock())
	testValues = make([][]indexed.RequiredOrOptional, count)
	for i := 0; i < count; i++ {
		var items []*Ydb.Value
		var vals []indexed.RequiredOrOptional
		for j := range result.Columns {
			v, val := valueFromPrimitiveTypeID(col[j], r)
			vals = append(vals, val)
			items = append(items, v)
		}
		result.Rows = append(result.Rows, &Ydb.Value{
			Items: items,
		})
		testValues[i] = vals
	}

	return result, testValues
}

func TestScanSqlTypes(t *testing.T) {
	s := initScanner()
	for _, test := range scannerData {
		t.Run(test.name, func(t *testing.T) {
			set, expected := getResultSet(test.count, test.columns)
			s.reset(set, test.setColumns...)
			for s.NextRow() {
				if test.columns[0].testDefault {
					if err := s.ScanWithDefaults(func() (values []indexed.Required) {
						for _, v := range test.values {
							values = append(values, v)
						}

						return values
					}()...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				} else {
					if err := s.Scan(test.values...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				}
				validateColumnIndexes(t, test, expected)
				expected = expected[1:]
			}
		})
	}
}

func TestScanNamed(t *testing.T) {
	s := initScanner()
	or := func(columns []string, i int, defaultValue string) string {
		if columns == nil {
			return defaultValue
		}

		return columns[i]
	}
	for _, test := range scannerData {
		t.Run(test.name, func(t *testing.T) {
			set, expected := getResultSet(test.count, test.columns)
			s.reset(set)
			for s.NextRow() {
				values := make([]named.Value, 0, len(test.values))
				if test.columns[0].testDefault {
					for i := range test.values {
						values = append(
							values,
							named.OptionalWithDefault(
								or(test.setColumns, i, test.columns[i].name),
								test.values[i],
							),
						)
					}
					if err := s.ScanNamed(values...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				} else {
					for i := range test.values {
						values = processValues(test, i, values, or)
					}
					if err := s.ScanNamed(values...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				}
				validateColumnIndexes(t, test, expected)
				expected = expected[1:]
			}
		})
	}
}

// validateColumnIndexes validates the column indexes of the test data against the expected values.
func validateColumnIndexes(t *testing.T, test struct {
	name             string
	count            int
	columns          []*column
	values           []indexed.RequiredOrOptional
	setColumns       []string
	setColumnIndexes []int
},
	expected [][]indexed.RequiredOrOptional,
) {
	if test.setColumnIndexes != nil {
		for i, v := range test.setColumnIndexes {
			require.Equal(t, expected[0][v], test.values[i])
		}
	} else {
		require.Equal(t, expected[0], test.values)
	}
}

// processValues processes the values based on the given test data and parameters.
func processValues(test struct {
	name             string
	count            int
	columns          []*column
	values           []indexed.RequiredOrOptional
	setColumns       []string
	setColumnIndexes []int
},
	i int,
	values []named.Value,
	or func(columns []string,
		i int,
		defaultValue string,
	) string,
) []named.Value {
	if test.columns[i].optional {
		if test.columns[i].testDefault {
			values = append(
				values,
				named.OptionalWithDefault(
					or(test.setColumns, i, test.columns[i].name),
					test.values[i],
				),
			)
		} else {
			values = append(
				values,
				named.Optional(
					or(test.setColumns, i, test.columns[i].name),
					test.values[i],
				),
			)
		}
	} else {
		values = append(
			values,
			named.Required(
				or(test.setColumns, i, test.columns[i].name),
				test.values[i],
			),
		)
	}

	return values
}

type jsonUnmarshaller struct {
	bytes []byte
}

func (json *jsonUnmarshaller) UnmarshalJSON(bytes []byte) error {
	json.bytes = bytes

	return nil
}

var _ json.Unmarshaler = &jsonUnmarshaller{}

func TestScanToJsonUnmarshaller(t *testing.T) {
	s := initScanner()
	for _, test := range []struct {
		name    string
		count   int
		columns []*column
		values  []indexed.RequiredOrOptional
	}{
		{
			name:  "<optional JSONDocument, required JSON> to json.Unmarshaller",
			count: 2,
			columns: []*column{{
				name:     "jsondocument",
				typeID:   Ydb.Type_JSON_DOCUMENT,
				optional: true,
			}, {
				name:   "json",
				typeID: Ydb.Type_JSON,
			}},
			values: []indexed.RequiredOrOptional{
				new(jsonUnmarshaller),
				new(jsonUnmarshaller),
			},
		}, {
			name:  "<required JSONDocument, optional JSON> to json.Unmarshaller",
			count: 2,
			columns: []*column{{
				name:   "jsondocument",
				typeID: Ydb.Type_JSON_DOCUMENT,
			}, {
				name:     "json",
				typeID:   Ydb.Type_JSON,
				optional: true,
			}},
			values: []indexed.RequiredOrOptional{
				new(jsonUnmarshaller),
				new(jsonUnmarshaller),
			},
		}, {
			name:  "<optional JSONDocument, optional JSON> to json.Unmarshaller",
			count: 2,
			columns: []*column{{
				name:     "jsondocument",
				typeID:   Ydb.Type_JSON_DOCUMENT,
				optional: true,
			}, {
				name:     "json",
				typeID:   Ydb.Type_JSON,
				optional: true,
			}},
			values: []indexed.RequiredOrOptional{
				new(jsonUnmarshaller),
				new(jsonUnmarshaller),
			},
		}, {
			name:  "<required JSONDocument, required JSON> to json.Unmarshaller",
			count: 2,
			columns: []*column{{
				name:   "jsondocument",
				typeID: Ydb.Type_JSON_DOCUMENT,
			}, {
				name:   "json",
				typeID: Ydb.Type_JSON,
			}},
			values: []indexed.RequiredOrOptional{
				new(jsonUnmarshaller),
				new(jsonUnmarshaller),
			},
		},
	} {
		set, _ := getResultSet(test.count, test.columns)
		s.reset(set)
		for s.NextRow() {
			values := make([]indexed.RequiredOrOptional, 0, len(test.values))
			for i, col := range test.columns {
				if col.optional {
					values = append(
						values,
						indexed.Optional(
							test.values[i],
						),
					)
				} else {
					values = append(
						values,
						indexed.Required(
							test.values[i],
						),
					)
				}
			}
			if err := s.Scan(values...); err != nil {
				t.Fatalf("test: %s; error: %s", test.name, err)
			}
		}
	}
}
