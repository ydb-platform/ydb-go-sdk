package scanner

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

//nolint:gocyclo
func valueFromPrimitiveTypeID(c *column, r xrand.Rand) (*Ydb.Value, any) {
	rv := r.Int64(math.MaxInt16)
	switch c.typeID {
	case Ydb.Type_BOOL:
		v := rv%2 == 1
		ydbval := Ydb.Value_builder{
			BoolValue: proto.Bool(v),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_INT8:
		v := int8(rv)
		ydbval := Ydb.Value_builder{
			Int32Value: proto.Int32(int32(v)),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_UINT8:
		if c.nilValue {
			ydbval := Ydb.Value_builder{
				NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
			}.Build()
			if c.testDefault {
				var dv uint8

				return ydbval, &dv
			}
			var dv *uint8

			return ydbval, &dv
		}
		v := uint8(rv)
		ydbval := Ydb.Value_builder{
			Uint32Value: proto.Uint32(uint32(v)),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_INT16:
		v := int16(rv)
		ydbval := Ydb.Value_builder{
			Int32Value: proto.Int32(int32(v)),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_UINT16:
		v := uint16(rv)
		ydbval := Ydb.Value_builder{
			Uint32Value: proto.Uint32(uint32(v)),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_INT32:
		if c.nilValue {
			ydbval := Ydb.Value_builder{
				NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
			}.Build()
			if c.testDefault {
				var dv int32

				return ydbval, &dv
			}
			var dv *int32

			return ydbval, &dv
		}
		v := int32(rv)
		ydbval := Ydb.Value_builder{
			Int32Value: proto.Int32(v),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_UINT32:
		v := uint32(rv)
		ydbval := Ydb.Value_builder{
			Uint32Value: proto.Uint32(v),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_INT64:
		v := rv
		ydbval := Ydb.Value_builder{
			Int64Value: proto.Int64(v),
		}.Build()
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
		ydbval := Ydb.Value_builder{
			Uint64Value: proto.Uint64(v),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_FLOAT:
		v := float32(rv)
		ydbval := Ydb.Value_builder{
			FloatValue: proto.Float32(v),
		}.Build()
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
		ydbval := Ydb.Value_builder{
			DoubleValue: proto.Float64(v),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_DATE:
		v := uint32(rv)
		ydbval := Ydb.Value_builder{
			Uint32Value: proto.Uint32(v),
		}.Build()
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
		ydbval := Ydb.Value_builder{
			Uint32Value: proto.Uint32(v),
		}.Build()
		src := value.DatetimeToTime(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_TIMESTAMP:
		v := uint64(rv)
		ydbval := Ydb.Value_builder{
			Uint64Value: proto.Uint64(v),
		}.Build()
		src := value.TimestampToTime(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_INTERVAL:
		if c.nilValue {
			ydbval := Ydb.Value_builder{
				NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
			}.Build()
			if c.testDefault {
				var dv time.Duration

				return ydbval, &dv
			}
			var dv *time.Duration

			return ydbval, &dv
		}
		rv %= time.Now().Unix()
		v := rv
		ydbval := Ydb.Value_builder{
			Int64Value: proto.Int64(v),
		}.Build()
		src := value.IntervalToDuration(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_TZ_DATE:
		v := time.Now().Format(value.LayoutDate) + ",Europe/Berlin"
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
		src, _ := value.TzDateToTime(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_TZ_DATETIME:
		if c.nilValue {
			ydbval := Ydb.Value_builder{
				NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
			}.Build()
			if c.testDefault {
				var dv time.Time

				return ydbval, &dv
			}
			var dv *time.Time

			return ydbval, &dv
		}
		rv %= time.Now().Unix()
		v := value.DatetimeToTime(uint32(rv)).Format(value.LayoutTzDatetime) + ",Europe/Berlin"
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
		src, _ := value.TzDatetimeToTime(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_TZ_TIMESTAMP:
		rv %= time.Now().Unix()
		v := value.TimestampToTime(uint64(rv)).Format(value.LayoutTzTimestamp) + ",Europe/Berlin"
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
		src, _ := value.TzTimestampToTime(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_STRING:
		if c.nilValue {
			ydbval := Ydb.Value_builder{
				NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
			}.Build()
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
		ydbval := Ydb.Value_builder{
			BytesValue: proto.ValueOrDefaultBytes(v),
		}.Build()
		src := v
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_UTF8:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_YSON:
		if c.nilValue {
			ydbval := Ydb.Value_builder{
				NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
			}.Build()
			if c.testDefault {
				var dv []byte

				return ydbval, &dv
			}
			var dv *[]byte

			return ydbval, &dv
		}
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
		src := []byte(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_JSON:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
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
			ydbval := Ydb.Value_builder{
				NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
			}.Build()
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
		ydbval := Ydb.Value_builder{
			High_128: high,
			Low_128:  proto.Uint64(low),
		}.Build()
		if c.optional && !c.testDefault {
			vp := &v

			return ydbval, &vp
		}

		return ydbval, &v
	case Ydb.Type_JSON_DOCUMENT:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
		src := []byte(v)
		if c.optional && !c.testDefault {
			vp := &src

			return ydbval, &vp
		}

		return ydbval, &src
	case Ydb.Type_DYNUMBER:
		v := strconv.FormatUint(uint64(rv), 10)
		ydbval := Ydb.Value_builder{
			TextValue: proto.String(v),
		}.Build()
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
		t := Ydb.Type_builder{
			TypeId: c.typeID.Enum(),
		}.Build()
		if c.optional {
			t = Ydb.Type_builder{
				OptionalType: Ydb.OptionalType_builder{
					Item: t,
				}.Build(),
			}.Build()
		}
		result.SetColumns(append(
			result.GetColumns(),
			Ydb.Column_builder{
				Name: c.name,
				Type: t,
			}.Build(),
		))
	}

	r := xrand.New(xrand.WithLock())
	testValues = make([][]indexed.RequiredOrOptional, count)
	for i := range count {
		var items []*Ydb.Value
		var vals []indexed.RequiredOrOptional
		for j := range result.GetColumns() {
			v, val := valueFromPrimitiveTypeID(col[j], r)
			vals = append(vals, val)
			items = append(items, v)
		}
		result.SetRows(append(result.GetRows(), Ydb.Value_builder{
			Items: items,
		}.Build()))
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

// jsonSQLScanner is a struct with a private json.RawMessage field that
// implements sql.Scanner and json.Marshaler without inheriting driver.Valuer.
// This mirrors the user-side pattern described in the fix for json.RawMessage binding.
type jsonSQLScanner struct {
	raw json.RawMessage
}

func (j jsonSQLScanner) MarshalJSON() ([]byte, error) {
	return j.raw, nil
}

func (j *jsonSQLScanner) UnmarshalJSON(data []byte) error {
	j.raw = data

	return nil
}

func (j *jsonSQLScanner) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		j.raw = nil
	case []byte:
		j.raw = v
	case string:
		j.raw = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into jsonSQLScanner", src)
	}

	return nil
}

// TestScanToSQLScannerJSON verifies that a type implementing sql.Scanner
// correctly receives []byte when scanning JSON and JSONDocument columns.
func TestScanToSQLScannerJSON(t *testing.T) {
	s := initScanner()
	for _, test := range []struct {
		name    string
		count   int
		columns []*column
	}{
		{
			name:  "<required JSON, required JSONDocument> to sql.Scanner",
			count: 5,
			columns: []*column{
				{name: "json", typeID: Ydb.Type_JSON},
				{name: "jsondocument", typeID: Ydb.Type_JSON_DOCUMENT},
			},
		},
		{
			name:  "<optional JSON, required JSONDocument> to sql.Scanner",
			count: 5,
			columns: []*column{
				{name: "json", typeID: Ydb.Type_JSON, optional: true},
				{name: "jsondocument", typeID: Ydb.Type_JSON_DOCUMENT},
			},
		},
		{
			name:  "<required JSON, optional JSONDocument> to sql.Scanner",
			count: 5,
			columns: []*column{
				{name: "json", typeID: Ydb.Type_JSON},
				{name: "jsondocument", typeID: Ydb.Type_JSON_DOCUMENT, optional: true},
			},
		},
		{
			name:  "<optional JSON, optional JSONDocument> to sql.Scanner",
			count: 5,
			columns: []*column{
				{name: "json", typeID: Ydb.Type_JSON, optional: true},
				{name: "jsondocument", typeID: Ydb.Type_JSON_DOCUMENT, optional: true},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			set, _ := getResultSet(test.count, test.columns)
			s.reset(set)
			for s.NextRow() {
				dst1 := new(jsonSQLScanner)
				dst2 := new(jsonSQLScanner)
				values := make([]indexed.RequiredOrOptional, len(test.columns))
				for i, col := range test.columns {
					var dst *jsonSQLScanner
					if i == 0 {
						dst = dst1
					} else {
						dst = dst2
					}
					if col.optional {
						values[i] = indexed.Optional(dst)
					} else {
						values[i] = indexed.Required(dst)
					}
				}
				require.NoError(t, s.Scan(values...))
				// Scan must have populated the raw bytes for each column.
				require.NotNil(t, dst1.raw, "json column: raw bytes must be set after Scan")
				require.NotNil(t, dst2.raw, "jsondocument column: raw bytes must be set after Scan")
				// The scanned bytes must be valid JSON (the test data is a numeric string).
				require.True(t, json.Valid(dst1.raw), "json column: scanned value must be valid JSON, got %q", dst1.raw)
				require.True(t, json.Valid(dst2.raw), "jsondocument column: scanned value must be valid JSON, got %q", dst2.raw)
			}
		})
	}
}
