package scanner

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

//nolint:gocyclo
func valueFromPrimitiveTypeID(c *column, r xrand.Rand) (*Ydb.Value, interface{}) {
	rv := r.Int64(math.MaxInt16)
	switch c.typeID {
	case Ydb.Type_BOOL:
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
	case Ydb.Type_INT8:
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
	case Ydb.Type_UINT8:
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
	case Ydb.Type_INT16:
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
	case Ydb.Type_UINT16:
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
	case Ydb.Type_INT32:
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
	case Ydb.Type_UINT32:
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
	case Ydb.Type_INT64:
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
	case Ydb.Type_UINT64:
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
	case Ydb.Type_FLOAT:
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
	case Ydb.Type_DOUBLE:
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
	case Ydb.Type_DATE:
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
	case Ydb.Type_DATETIME:
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
	case Ydb.Type_TIMESTAMP:
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
	case Ydb.Type_INTERVAL:
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
	case Ydb.Type_TZ_DATE:
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
	case Ydb.Type_TZ_DATETIME:
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
	case Ydb.Type_TZ_TIMESTAMP:
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
	case Ydb.Type_STRING:
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
	case Ydb.Type_UTF8:
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
	case Ydb.Type_YSON:
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
	case Ydb.Type_JSON:
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
	case Ydb.Type_UUID:
		if c.nilValue {
			ydbval := &Ydb.Value{
				Value: &Ydb.Value_NullFlagValue{},
			}
			if c.testDefault {
				var dv uuid.UUID

				return ydbval, &dv
			}
			var dv *uuid.UUID

			return ydbval, &dv
		}
		v := uuid.UUID{}

		binary.LittleEndian.PutUint64(v[0:8], uint64(rv))
		binary.LittleEndian.PutUint64(v[8:16], uint64(rv))
		low, high := value.UUIDToHiLoPair(v)
		ydbval := &Ydb.Value{
			High_128: high,
			Value: &Ydb.Value_Low_128{
				Low_128: low,
			},
		}
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_JSON_DOCUMENT:
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
	case Ydb.Type_DYNUMBER:
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
	default:
		panic("ydb: unexpected types")
	}
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
			result.GetColumns(),
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
		for j := range result.GetColumns() {
			v, val := valueFromPrimitiveTypeID(col[j], r)
			vals = append(vals, val)
			items = append(items, v)
		}
		result.Rows = append(result.GetRows(), &Ydb.Value{
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
				if test.setColumnIndexes != nil {
					for i, v := range test.setColumnIndexes {
						require.Equal(t, expected[0][v], test.values[i])
					}
				} else {
					require.Equal(t, expected[0], test.values)
				}
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
				//nolint:nestif
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
					}
					if err := s.ScanNamed(values...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				}
				if test.setColumnIndexes != nil {
					for i, v := range test.setColumnIndexes {
						require.Equal(t, expected[0][v], test.values[i])
					}
				} else {
					require.Equal(t, expected[0], test.values)
				}
				expected = expected[1:]
			}
		})
	}
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
