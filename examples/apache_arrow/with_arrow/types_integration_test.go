//go:build integration

package witharrow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestArrowYQLScalars(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	for _, test := range scalarFixtures() {
		t.Run(test.values[0].Type().Yql(), func(t *testing.T) {
			for _, mode := range []string{"required", "optional", "all-null", "empty"} {
				t.Run(mode, func(t *testing.T) {
					want := test.values
					goType := test.goType
					switch mode {
					case "optional":
						want = []types.Value{types.NullValue(want[0].Type())}
						for _, v := range test.values {
							want = append(want, types.OptionalValue(v), types.NullValue(v.Type()))
						}
						goType = reflect.PointerTo(goType)
					case "all-null":
						want = []types.Value{types.NullValue(want[0].Type()), types.NullValue(want[0].Type())}
						goType = reflect.PointerTo(goType)
					}
					statement := scalarSelect(want)
					logicalType := want[0].Type()
					if mode == "empty" {
						statement = strings.Replace(statement, " ORDER BY id;", " WHERE FALSE ORDER BY id;", 1)
						want = nil
					}
					if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
						for _, format := range []string{"Ydb.Value", "WithArrow"} {
							t.Run(format, func(t *testing.T) {
								alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
								defer alloc.AssertSize(t, 0)
								var calls atomic.Int32
								factory := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
									calls.Add(1)

									return ipc.NewReader(part, opts...)
								}
								option := query.WithYdbValue()
								if format == "WithArrow" {
									option = query.WithArrow(factory, ipc.WithAllocator(alloc))
								}
								assertScalarRows(ctx, t, s, statement, option, logicalType, goType, want)
								if format == "WithArrow" && len(want) > 0 && calls.Load() == 0 {
									t.Fatal("server did not return Arrow IPC")
								}
							})
						}

						return nil
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestArrowYQLAliases(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	for _, test := range []struct {
		name, expression string
		want             types.Value
		goType           reflect.Type
	}{
		{"Text", `CAST("test" AS Text)`, types.OptionalValue(types.TextValue("test")), reflect.TypeFor[*string]()},
		{"Bytes", `CAST("test" AS Bytes)`, types.BytesValueFromString("test"), reflect.TypeFor[[]byte]()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
				for _, option := range []query.ExecuteOption{query.WithYdbValue(), query.WithArrow(ipc.NewReader)} {
					assertScalarRows(ctx, t, s, "SELECT "+test.expression+" AS value;", option,
						test.want.Type(), test.goType, []types.Value{test.want})
				}

				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArrowYQLUnsupportedDecoderTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	for _, test := range unsupportedTypeFixtures() {
		t.Run(test.name, func(t *testing.T) {
			statement := "SELECT " + test.expression + " AS value;"
			baseline, err := db.Query().QueryResultSet(ctx, statement, query.WithYdbValue())
			if err != nil {
				t.Fatal(err)
			}
			defer baseline.Close(ctx)
			if columns := baseline.ColumnTypes(); len(columns) != 1 || columns[0].Yql() != test.logicalType {
				t.Fatalf("protobuf column types=%v, want %s", columns, test.logicalType)
			}
			row, err := baseline.NextRow(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if values := row.Values(); len(values) != 1 {
				t.Fatalf("protobuf values=%v, want one value", values)
			}
			if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
				assertRawArrowType(ctx, t, s, statement, test.physicalType)
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				var calls atomic.Int32
				factory := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
					calls.Add(1)

					return ipc.NewReader(part, opts...)
				}
				res, err := s.Query(ctx, statement, query.WithArrow(factory, ipc.WithAllocator(alloc)))
				if err == nil {
					defer res.Close(ctx)
					var rs query.ResultSet
					rs, err = res.NextResultSet(ctx)
					if err == nil {
						_, err = rs.NextRow(ctx)
					}
				}
				wantError := decoderTypeError(test.logicalType, test.physicalType)
				if err == nil || !strings.Contains(err.Error(), wantError) || calls.Load() == 0 {
					t.Fatalf("decode calls=%d, error=%v; want %q", calls.Load(), err, wantError)
				}

				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueryArrowYQLTypesOutsideRowAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	// The SDK's Ydb.Value type conversion does not support these wire types.
	for _, test := range []struct {
		name, expression string
		physicalType     arrow.Type
	}{
		{"EmptyList", `[]`, arrow.STRUCT},
		{"EmptyDict", `AsDict()`, arrow.STRUCT},
		{"Tagged", `AsTagged(42, "tag")`, arrow.INT32},
		{"TzDate32", `TzDate32("1969-12-31,Europe/Moscow")`, arrow.STRUCT},
		{"TzDatetime64", `TzDatetime64("1969-12-31T12:34:56,Europe/Moscow")`, arrow.STRUCT},
		{"TzTimestamp64", `TzTimestamp64("1969-12-31T12:34:56.123456,Europe/Moscow")`, arrow.STRUCT},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
				assertRawArrowType(ctx, t, s, "SELECT "+test.expression+" AS value;", test.physicalType)

				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArrowYQLNonPersistableResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	for _, test := range []struct {
		name   string
		option query.ExecuteOption
	}{
		{"Ydb.Value", query.WithYdbValue()},
		{"WithArrow", query.WithArrow(ipc.NewReader)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := db.Query().QueryRow(ctx, `SELECT ParseTypeHandle("Int32") AS value;`, test.option)
			if !ydb.IsOperationError(err, Ydb.StatusIds_GENERIC_ERROR) ||
				!strings.Contains(err.Error(), "Persistable required") {
				t.Fatalf("resource result: %v", err)
			}
		})
	}
}

type scalarFixture struct {
	goType reflect.Type
	values []types.Value
}

func scalarFixtures() []scalarFixture {
	return []scalarFixture{
		{reflect.TypeFor[bool](), []types.Value{types.BoolValue(false), types.BoolValue(true)}},
		{reflect.TypeFor[int8](), []types.Value{
			types.Int8Value(math.MinInt8), types.Int8Value(0), types.Int8Value(math.MaxInt8),
		}},
		{reflect.TypeFor[int16](), []types.Value{
			types.Int16Value(math.MinInt16), types.Int16Value(0), types.Int16Value(math.MaxInt16),
		}},
		{reflect.TypeFor[int32](), []types.Value{
			types.Int32Value(math.MinInt32), types.Int32Value(0), types.Int32Value(math.MaxInt32),
		}},
		{reflect.TypeFor[int64](), []types.Value{
			types.Int64Value(math.MinInt64), types.Int64Value(0), types.Int64Value(math.MaxInt64),
		}},
		{reflect.TypeFor[uint8](), []types.Value{types.Uint8Value(0), types.Uint8Value(math.MaxUint8)}},
		{reflect.TypeFor[uint16](), []types.Value{types.Uint16Value(0), types.Uint16Value(math.MaxUint16)}},
		{reflect.TypeFor[uint32](), []types.Value{types.Uint32Value(0), types.Uint32Value(math.MaxUint32)}},
		{reflect.TypeFor[uint64](), []types.Value{types.Uint64Value(0), types.Uint64Value(math.MaxUint64)}},
		{reflect.TypeFor[float32](), []types.Value{
			types.FloatValue(-math.MaxFloat32), types.FloatValue(math.SmallestNonzeroFloat32), types.FloatValue(1.25),
			types.FloatValue(float32(math.Copysign(0, -1))), types.FloatValue(float32(math.Inf(1))),
			types.FloatValue(float32(math.Inf(-1))), types.FloatValue(float32(math.NaN())),
		}},
		{reflect.TypeFor[float64](), []types.Value{
			types.DoubleValue(-math.MaxFloat64), types.DoubleValue(math.SmallestNonzeroFloat64), types.DoubleValue(1.25),
			types.DoubleValue(math.Copysign(0, -1)), types.DoubleValue(math.Inf(1)),
			types.DoubleValue(math.Inf(-1)), types.DoubleValue(math.NaN()),
		}},
		{reflect.TypeFor[[]byte](), []types.Value{
			types.BytesValueFromString(""), types.BytesValue([]byte{0, 1, 0x7f, 0x80, 0xff}), types.BytesValueFromString("test"),
		}},
		{reflect.TypeFor[string](), []types.Value{
			types.TextValue(""), types.TextValue("test"), types.TextValue("Привет, 世界 👋"), types.TextValue("a\x00b"),
		}},
	}
}

func scalarSelect(values []types.Value) string {
	rows := make([]string, len(values))
	for i, v := range values {
		rows[i] = fmt.Sprintf("AsStruct(%d AS id, %s AS value)", i, v.Yql())
	}

	return "SELECT value FROM AS_TABLE([" + strings.Join(rows, ", ") + "]) ORDER BY id;"
}

func assertScalarRows(ctx context.Context, t *testing.T, executor query.Executor, statement string,
	option query.ExecuteOption, logicalType types.Type, goType reflect.Type, want []types.Value,
) {
	t.Helper()
	rs, err := executor.QueryResultSet(ctx, statement, option)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close(ctx)
	if columns := rs.Columns(); !reflect.DeepEqual(columns, []string{"value"}) {
		t.Fatalf("columns=%v", columns)
	}
	if columns := rs.ColumnTypes(); len(columns) != 1 || !types.Equal(columns[0], logicalType) {
		t.Fatalf("column types=%v, want %s", columns, logicalType)
	}
	dst := reflect.New(goType)
	structDst := reflect.New(reflect.StructOf([]reflect.StructField{{Name: "Value", Type: goType, Tag: `sql:"value"`}}))
	var count int
	for row, err := range rs.Rows(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if count >= len(want) {
			t.Fatal("unexpected extra row")
		}
		v := want[count]
		expected := reflect.New(goType)
		if err := types.CastTo(v, expected.Interface()); err != nil {
			t.Fatal(err)
		}
		for _, scan := range []struct {
			name string
			scan func() error
		}{
			{"Scan", func() error { return row.Scan(dst.Interface()) }},
			{"ScanNamed", func() error { return row.ScanNamed(query.Named("value", dst.Interface())) }},
		} {
			if err := scan.scan(); err != nil {
				t.Fatalf("%s row %d: %v", scan.name, count, err)
			}
			if !equalScalar(dst.Elem(), expected.Elem()) {
				t.Fatalf("%s row %d: got %v, want %v", scan.name, count, dst.Elem(), expected.Elem())
			}
		}
		if err := row.ScanStruct(structDst.Interface()); err != nil {
			t.Fatal(err)
		}
		if !equalScalar(structDst.Elem().Field(0), expected.Elem()) {
			t.Fatalf("ScanStruct row %d: got %v, want %v", count, structDst.Elem(), expected.Elem())
		}
		if values := row.Values(); len(values) != 1 ||
			!types.Equal(values[0].Type(), v.Type()) || values[0].Yql() != v.Yql() {
			t.Fatalf("Values row %d: got %v, want %s", count, values, v.Yql())
		}
		var scanned types.Value
		if err := row.Scan(&scanned); err != nil {
			t.Fatal(err)
		}
		if !types.Equal(scanned.Type(), v.Type()) || scanned.Yql() != v.Yql() {
			t.Fatalf("Scan types.Value row %d: got %s, want %s", count, scanned.Yql(), v.Yql())
		}
		count++
	}
	if count != len(want) {
		t.Fatalf("rows=%d, want %d", count, len(want))
	}
	if err := rs.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func equalScalar(a, b reflect.Value) bool {
	if a.Kind() == reflect.Pointer {
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}

		return equalScalar(a.Elem(), b.Elem())
	}
	if a.Kind() == reflect.Float32 || a.Kind() == reflect.Float64 {
		return (math.IsNaN(a.Float()) && math.IsNaN(b.Float())) || math.Float64bits(a.Float()) == math.Float64bits(b.Float())
	}

	if a.Kind() == reflect.Slice {
		return bytes.Equal(a.Bytes(), b.Bytes())
	}

	return reflect.DeepEqual(a.Interface(), b.Interface())
}

func assertRawArrowType(ctx context.Context, t *testing.T, s query.Session, statement string, physicalType arrow.Type) {
	t.Helper()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	res, err := s.QueryArrow(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close(ctx)
	var rows int64
	for part, err := range res.Parts(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		reader, err := ipc.NewReader(part, ipc.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer reader.Release()
			for {
				batch, err := reader.Read()
				if errors.Is(err, io.EOF) {
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if batch.NumCols() != 1 || batch.ColumnName(0) != "value" || batch.Column(0).DataType().ID() != physicalType {
					t.Fatalf("Arrow schema=%s, want value with type %s", batch.Schema(), physicalType)
				}
				rows += batch.NumRows()
			}
		}()
	}
	if rows != 1 {
		t.Fatalf("Arrow rows=%d, want 1", rows)
	}
	if err := res.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type unsupportedTypeFixture struct {
	name, expression, logicalType string
	physicalType                  arrow.Type
}

func unsupportedTypeFixtures() []unsupportedTypeFixture {
	return []unsupportedTypeFixture{
		{"Yson", `Yson("{value=1;}")`, "Yson", arrow.BINARY},
		{"Json", `Json("{\"value\":1}")`, "Json", arrow.STRING},
		{"JsonDocument", `JsonDocument("{\"value\":1}")`, "JsonDocument", arrow.STRING},
		{"Uuid", `Uuid("12345678-9abc-def0-1234-56789abcdef0")`, "Uuid", arrow.FIXED_SIZE_BINARY},
		{"DyNumber", `DyNumber("1.23")`, "DyNumber", arrow.STRING},
		{"Date", `Date("2000-01-01")`, "Date", arrow.UINT16},
		{"Datetime", `Datetime("2000-01-01T12:34:56Z")`, "Datetime", arrow.UINT32},
		{"Timestamp", `Timestamp("2000-01-01T12:34:56.123456Z")`, "Timestamp", arrow.UINT64},
		{"Interval", `Interval("PT1.123456S")`, "Interval", arrow.INT64},
		{"Date32", `Date32("1969-12-31")`, "Date32", arrow.INT32},
		{"Datetime64", `Datetime64("1969-12-31T12:34:56Z")`, "Datetime64", arrow.INT64},
		{"Timestamp64", `Timestamp64("1969-12-31T12:34:56.123456Z")`, "Timestamp64", arrow.INT64},
		{"Interval64", `Interval64("-PT1.123456S")`, "Interval64", arrow.INT64},
		{"TzDate", `TzDate("2000-01-01,Europe/Moscow")`, "TzDate", arrow.STRUCT},
		{"TzDatetime", `TzDatetime("2000-01-01T12:34:56,Europe/Moscow")`, "TzDatetime", arrow.STRUCT},
		{"TzTimestamp", `TzTimestamp("2000-01-01T12:34:56.123456,Europe/Moscow")`, "TzTimestamp", arrow.STRUCT},
		{"Decimal22", `Decimal("1.23",22,9)`, "Decimal(22,9)", arrow.FIXED_SIZE_BINARY},
		{"Decimal31", `Decimal("1.23",31,9)`, "Decimal(31,9)", arrow.FIXED_SIZE_BINARY},
		{"Decimal35", `Decimal("12345678901234567890123456789012345",35,0)`, "Decimal(35,0)", arrow.FIXED_SIZE_BINARY},
		{"NestedOptionalPresent", `Just(Just(42))`, "Optional<Optional<Int32>>", arrow.STRUCT},
		{"NestedOptionalInnerNull", `Just(Nothing(Int32?))`, "Optional<Optional<Int32>>", arrow.STRUCT},
		{"NestedOptionalOuterNull", `Nothing(Int32??)`, "Optional<Optional<Int32>>", arrow.STRUCT},
		{"List", `[1,2]`, "List<Int32>", arrow.LIST},
		{"EmptyTypedList", `ListCreate(Int32)`, "List<Int32>", arrow.LIST},
		{"Tuple", `AsTuple(1,"text")`, "Tuple<Int32,String>", arrow.STRUCT},
		{"Struct", `AsStruct(1 AS id,"text" AS name)`, "Struct<'id':Int32,'name':String>", arrow.STRUCT},
		{"Dict", `AsDict(AsTuple("key",1))`, "Dict<String,Int32>", arrow.LIST},
		{"Set", `AsSet("key")`, "Set<String>", arrow.LIST},
		{"VariantTuple", `Variant(42,"0",Variant<Int32,Utf8>)`, "Variant<Int32,Utf8>", arrow.DENSE_UNION},
		{
			"VariantStruct",
			`Variant(42,"id",Variant<id:Int32,name:Utf8>)`, "Variant<'id':Int32,'name':Utf8>", arrow.DENSE_UNION,
		},
		{"Null", `NULL`, "Null", arrow.NULL},
		{"Void", `Void()`, "Void", arrow.STRUCT},
		{"OptionalDateNull", `Nothing(Date?)`, "Optional<Date>", arrow.UINT16},
		{"OptionalJsonNull", `Nothing(Json?)`, "Optional<Json>", arrow.STRING},
		{"OptionalListNull", `Nothing(List<Int32>?)`, "Optional<List<Int32>>", arrow.LIST},
		{"PgInt4", `PgInt4("42")`, "PgType(23)", arrow.STRING},
		{"PgText", `PgText("test")`, "PgType(25)", arrow.STRING},
		{"PgNumeric", `PgNumeric("1.23")`, "PgType(1700)", arrow.STRING},
		{"PgArray", `PgArray(1p,NULL,2p)`, "PgType(1007)", arrow.STRING},
	}
}

func decoderTypeError(logicalType string, physicalType arrow.Type) string {
	switch physicalType {
	case arrow.STRUCT, arrow.LIST, arrow.DENSE_UNION:

		return "unsupported Arrow array"
	case arrow.NULL:

		return "null in non-optional Null"
	default:
		inner := strings.TrimSuffix(strings.TrimPrefix(logicalType, "Optional<"), ">")

		return "does not match YDB " + inner
	}
}
