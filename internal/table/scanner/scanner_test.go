package scanner

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/rand"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/timeutil"
	public "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

// nolint:gocyclo
func valueFromPrimitiveTypeID(c *column) (*Ydb.Value, interface{}) {
	rv := rand.Int64(math.MaxInt16)
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
		src := timeutil.UnmarshalDate(v)
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
		src := timeutil.UnmarshalDatetime(v)
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
		src := timeutil.UnmarshalTimestamp(v)
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
		src := timeutil.UnmarshalInterval(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_TZ_DATE:
		rv %= (time.Now().Unix() / 24 / 60 / 60)
		v := timeutil.MarshalTzDate(timeutil.UnmarshalDate(uint32(rv)))
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src, _ := timeutil.UnmarshalTzDate(v)
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
		v := timeutil.MarshalTzDatetime(timeutil.UnmarshalDatetime(uint32(rv)))
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src, _ := timeutil.UnmarshalTzDatetime(v)
		if c.optional && !c.testDefault {
			vp := &src
			return ydbval, &vp
		}
		return ydbval, &src
	case Ydb.Type_TZ_TIMESTAMP:
		rv %= time.Now().Unix()
		v := timeutil.MarshalTzTimestamp(timeutil.UnmarshalTimestamp(uint64(rv)))
		ydbval := &Ydb.Value{
			Value: &Ydb.Value_TextValue{
				TextValue: v,
			},
		}
		src, _ := timeutil.UnmarshalTzTimestamp(v)
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

func getResultSet(count int, col []*column) (r *Ydb.ResultSet, testValues [][]interface{}) {
	r = &Ydb.ResultSet{}
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
		r.Columns = append(
			r.Columns,
			&Ydb.Column{
				Name: c.name,
				Type: t,
			},
		)
	}

	testValues = make([][]interface{}, count)
	for i := 0; i < count; i++ {
		var items []*Ydb.Value
		var vals []interface{}
		for j := range r.Columns {
			v, val := valueFromPrimitiveTypeID(col[j])
			vals = append(vals, val)
			items = append(items, v)
		}
		r.Rows = append(r.Rows, &Ydb.Value{
			Items: items,
		})
		testValues[i] = vals
	}
	return r, testValues
}

func TestScanSqlTypes(t *testing.T) {
	s := initScanner()
	for _, test := range scannerData {
		t.Run(test.name, func(t *testing.T) {
			set, expected := getResultSet(test.count, test.columns)
			s.reset(set, test.setColumns...)
			for s.NextRow() {
				if test.columns[0].testDefault {
					if err := s.ScanWithDefaults(test.values...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				} else {
					if err := s.Scan(test.values...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				}
				if test.setColumnIndexes != nil {
					for i, v := range test.setColumnIndexes {
						testutil.Equal(t, expected[0][v], test.values[i])
					}
				} else {
					testutil.Equal(t, expected[0], test.values)
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
				values := make([]public.NamedValue, 0, len(test.values))
				if test.columns[0].testDefault {
					for i := range test.values {
						values = append(
							values,
							public.NamedWithDefault(
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
						values = append(
							values,
							public.Named(
								or(test.setColumns, i, test.columns[i].name),
								test.values[i],
							),
						)
					}
					if err := s.ScanNamed(values...); err != nil {
						t.Fatalf("test: %s; error: %s", test.name, err)
					}
				}
				if test.setColumnIndexes != nil {
					for i, v := range test.setColumnIndexes {
						testutil.Equal(t, expected[0][v], test.values[i])
					}
				} else {
					testutil.Equal(t, expected[0], test.values)
				}
				expected = expected[1:]
			}
		})
	}
}
