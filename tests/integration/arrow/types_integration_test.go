//go:build integration

package witharrow

import (
	"bytes"
	"context"
	"database/sql/driver"
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
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
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
						for _, format := range []string{"Ydb.Value", "WithResultFormatArrow"} {
							t.Run(format, func(t *testing.T) {
								alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
								defer alloc.AssertSize(t, 0)
								var calls atomic.Int32
								factory := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
									calls.Add(1)

									return ipc.NewReader(part, opts...)
								}
								option := query.WithYdbValue()
								if format == "WithResultFormatArrow" {
									option = query.WithResultFormatArrow(factory, ipc.WithAllocator(alloc))
								}
								assertScalarRows(ctx, t, s, statement, option, logicalType, goType, want)
								if format == "WithResultFormatArrow" && len(want) > 0 && calls.Load() == 0 {
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
				for _, option := range []query.ExecuteOption{query.WithYdbValue(), query.WithResultFormatArrow(ipc.NewReader)} {
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

func TestArrowYQLTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, test := range fullTypeFixtures() {
		t.Run(test.name, func(t *testing.T) {
			for _, mode := range []string{"required", "optional", "null"} {
				t.Run(mode, func(t *testing.T) {
					expression := test.expression
					switch mode {
					case "optional":
						expression = "Just(" + expression + ")"
					case "null":
						expression = "Nothing(OptionalType(TypeOf(" + expression + ")))"
					}
					statement := "SELECT " + expression + " AS value;"
					baseline, err := db.Query().QueryRow(ctx, statement, query.WithYdbValue())
					if err != nil {
						t.Fatal(err)
					}
					want := baseline.Values()
					if mode == "required" && want[0].Type().Yql() != test.logicalType {
						t.Fatalf("logical type=%s, want %s", want[0].Type().Yql(), test.logicalType)
					}
					if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
						if mode == "required" {
							assertRawArrowType(ctx, t, s, statement, test.physicalType)
						}
						assertValueRows(ctx, t, s, statement, want)

						return nil
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestArrowYQLTimezones(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, kind := range []string{"TzDate", "TzDatetime", "TzTimestamp", "TzDate32", "TzDatetime64", "TzTimestamp64"} {
		for _, zone := range []string{
			"UTC", "Europe/Moscow", "America/New_York", "Pacific/Kiritimati", "Pacific/Honolulu", "Pacific/Apia",
		} {
			days := []string{"2000-01-01", "2021-03-14", "2021-11-07"}
			if kind == "TzDate32" || kind == "TzDatetime64" || kind == "TzTimestamp64" {
				days = append(days, "0001-01-01", "-0001-01-01", "10000-01-01")
			}
			for _, day := range days {
				t.Run(kind+"/"+zone+"/"+day, func(t *testing.T) {
					literal := day
					if strings.Contains(kind, "Datetime") {
						literal += "T12:34:56"
					} else if strings.Contains(kind, "Timestamp") {
						literal += "T12:34:56.123456"
					}
					statement := fmt.Sprintf("SELECT %s(%q) AS value;", kind, literal+","+zone)
					row, err := db.Query().QueryRow(ctx, statement, query.WithYdbValue())
					if err != nil {
						t.Fatal(err)
					}
					if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
						assertValueRows(ctx, t, s, statement, row.Values())

						return nil
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestArrowWideTimezoneScans(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, test := range []struct{ kind, text string }{
		{"TzDate32", "-0001-02-29,UTC"},
		{"TzDate32", "-144169-01-01,UTC"},
		{"TzDatetime64", "10000-02-29T12:34:56,UTC"},
		{"TzDatetime64", "148107-12-31T23:59:59,UTC"},
		{"TzTimestamp64", "1969-12-31T12:34:56.123456,Europe/Moscow"},
		{"TzTimestamp64", "148107-12-31T23:59:59.999999,UTC"},
	} {
		t.Run(test.kind+"/"+test.text, func(t *testing.T) {
			statement := fmt.Sprintf("SELECT %s(%q) AS required, Just(%s(%q)) AS optional, Nothing(%s?) AS absent;",
				test.kind, test.text, test.kind, test.text, test.kind)
			baseline, err := db.Query().QueryRow(ctx, statement, query.WithYdbValue())
			if err != nil {
				t.Fatal(err)
			}
			type nativeTimes struct {
				Required time.Time  `sql:"required"`
				Optional *time.Time `sql:"optional"`
				Absent   *time.Time `sql:"absent"`
			}
			var want nativeTimes
			if err := baseline.ScanStruct(&want); err != nil {
				t.Fatal(err)
			}
			row, err := db.Query().QueryRow(ctx, statement, query.WithResultFormatArrow(ipc.NewReader))
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"Scan", "ScanNamed", "ScanStruct"} {
				var got nativeTimes
				got.Absent = &got.Required
				switch mode {
				case "Scan":
					err = row.Scan(&got.Required, &got.Optional, &got.Absent)
				case "ScanNamed":
					err = row.ScanNamed(query.Named("absent", &got.Absent),
						query.Named("optional", &got.Optional), query.Named("required", &got.Required))
				case "ScanStruct":
					err = row.ScanStruct(&got)
				}
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: got %+v (%v), want %+v", mode, got, err, want)
				}
			}
		})
	}
}

func TestArrowYQLExecutors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	fields := make([]string, 0, len(fullTypeFixtures()))
	for i, test := range fullTypeFixtures() {
		fields = append(fields, fmt.Sprintf("%s AS v%d", test.expression, i))
	}
	statement := "SELECT " + strings.Join(fields, ", ") + ";"
	baseline, err := db.Query().QueryRow(ctx, statement, query.WithYdbValue())
	if err != nil {
		t.Fatal(err)
	}
	want := baseline.Values()
	verify := func(t *testing.T, executor query.Executor) {
		for _, method := range []string{"Query", "QueryRow", "QueryResultSet"} {
			t.Run(method, func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				option := query.WithResultFormatArrow(ipc.NewReader, ipc.WithAllocator(alloc))
				var saved []types.Value
				switch method {
				case "QueryRow":
					row, err := executor.QueryRow(ctx, statement, option)
					if err != nil {
						t.Fatal(err)
					}
					saved = assertWideRow(t, row, want)
				case "QueryResultSet":
					result, err := executor.QueryResultSet(ctx, statement, option)
					if err != nil {
						t.Fatal(err)
					}
					defer result.Close(ctx)
					row, err := result.NextRow(ctx)
					if err != nil {
						t.Fatal(err)
					}
					saved = assertWideRow(t, row, want)
					if _, err := result.NextRow(ctx); !errors.Is(err, io.EOF) {
						t.Fatalf("extra row: %v", err)
					}
					if err := result.Close(ctx); err != nil {
						t.Fatal(err)
					}
				case "Query":
					result, err := executor.Query(ctx, statement, option)
					if err != nil {
						t.Fatal(err)
					}
					defer result.Close(ctx)
					var sets, rows int
					for rs, err := range result.ResultSets(ctx) {
						if err != nil {
							t.Fatal(err)
						}
						sets++
						for row, err := range rs.Rows(ctx) {
							if err != nil {
								t.Fatal(err)
							}
							rows++
							saved = assertWideRow(t, row, want)
						}
					}
					if sets != 1 || rows != 1 {
						t.Fatalf("sets=%d, rows=%d", sets, rows)
					}
					if err := result.Close(ctx); err != nil {
						t.Fatal(err)
					}
				}
				assertValues(t, saved, want)
			})
		}
	}
	t.Run("Client", func(t *testing.T) { verify(t, db.Query()) })
	t.Run("Session", func(t *testing.T) {
		if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
			verify(t, s)

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("TxActor", func(t *testing.T) {
		if err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			verify(t, tx)

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
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
		{"WithResultFormatArrow", query.WithResultFormatArrow(ipc.NewReader)},
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

func TestArrowYQLParts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	var fields []string
	for i, fixture := range fullTypeFixtures() {
		fields = append(fields, fmt.Sprintf(
			"IF(id %% 3 == 0, Nothing(OptionalType(TypeOf(%s))), Just(%s)) AS v%d",
			fixture.expression, fixture.expression, i,
		))
	}
	fields = append(fields, `IF(id % 2 == 0, Variant(42,"0",Variant<Int32,Utf8>), `+
		`Variant("other"u,"1",Variant<Int32,Utf8>)) AS variant`)
	fields = append(fields, fmt.Sprintf(`"%s" AS payload`, strings.Repeat("x", 1024)))
	statement := "SELECT " + strings.Join(fields, ", ") +
		" FROM AS_TABLE(ListMap(ListFromRange(0, 120), ($x) -> { RETURN AsStruct($x AS id); })) ORDER BY id;"
	statement += statement
	baseline, err := db.Query().Query(ctx, statement, query.WithYdbValue())
	if err != nil {
		t.Fatal(err)
	}
	want := readResultValues(ctx, t, baseline, 120)
	if err := baseline.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(want) != 2 {
		t.Fatalf("baseline result sets=%d", len(want))
	}
	for _, materialized := range []bool{false, true} {
		for _, prefetch := range []int{0, 4} {
			for _, earlyClose := range []bool{false, true} {
				name := fmt.Sprintf("materialized=%t/prefetch=%d/earlyClose=%t", materialized, prefetch, earlyClose)
				t.Run(name, func(t *testing.T) {
					alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer alloc.AssertSize(t, 0)
					var calls atomic.Int32
					factory := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
						calls.Add(1)

						return ipc.NewReader(part, opts...)
					}
					read := func(executor query.Executor) error {
						result, err := executor.Query(ctx, statement, query.WithResultFormatArrow(factory, ipc.WithAllocator(alloc)),
							query.WithResponsePartLimitSizeBytes(4<<10), query.WithResponsePartPrefetch(prefetch))
						if err != nil {
							return err
						}
						defer result.Close(ctx)
						var saved [][]types.Value
						sets := 0
					results:
						for rs, err := range result.ResultSets(ctx) {
							if err != nil {
								return err
							}
							rows := 0
							for row, err := range rs.Rows(ctx) {
								if err != nil {
									return err
								}
								if sets >= len(want) || rows >= len(want[sets]) {
									t.Fatal("extra result")
								}
								expected := want[sets][rows]
								values := row.Values()
								assertValues(t, values, expected)
								scanned := make([]types.Value, len(expected))
								destinations := make([]any, len(expected))
								for i := range destinations {
									destinations[i] = &scanned[i]
								}
								if err := row.Scan(destinations...); err != nil {
									return err
								}
								assertValues(t, scanned, expected)
								if rows < 3 {
									saved = append(saved, values)
								}
								rows++
								if earlyClose && rows == 3 {
									break results
								}
							}
							if rows != len(want[sets]) {
								t.Fatalf("rows=%d", rows)
							}
							sets++
						}
						if !earlyClose && (sets != 2 || calls.Load() <= 2) {
							t.Fatalf("sets=%d, IPC parts=%d", sets, calls.Load())
						}
						if err := result.Close(ctx); err != nil {
							return err
						}
						for i, values := range saved {
							assertValues(t, values, want[i/3][i%3])
						}
						t.Logf("IPC parts=%d", calls.Load())

						return nil
					}
					if materialized {
						err = read(db.Query())
					} else {
						err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error { return read(s) })
					}
					if err != nil {
						t.Fatal(err)
					}
				})
			}
		}
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

type fullTypeFixture struct {
	name, expression, logicalType string
	physicalType                  arrow.Type
}

func fullTypeFixtures() []fullTypeFixture {
	return []fullTypeFixture{
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
		{"NestedOptional3", `Just(Just(Just(42)))`, "Optional<Optional<Optional<Int32>>>", arrow.STRUCT},
		{"NestedOptional3InnerNull", `Just(Just(Nothing(Int32?)))`, "Optional<Optional<Optional<Int32>>>", arrow.STRUCT},
		{"OptionalList", `[Just(1),Nothing(Int32?),Just(2)]`, "List<Optional<Int32>>", arrow.LIST},
		{"NestedList", `[[1,2],ListCreate(Int32),[3]]`, "List<List<Int32>>", arrow.LIST},
		{"EmptyTypedDict", `DictCreate(String,Int32)`, "Dict<String,Int32>", arrow.LIST},
		{"EmptyTypedSet", `DictCreate(String,Void)`, "Set<String>", arrow.LIST},
		{"VariantTupleSecond", `Variant("text"u,"1",Variant<Int32,Utf8>)`, "Variant<Int32,Utf8>", arrow.DENSE_UNION},
		{
			"VariantStructSecond", `Variant("text"u,"name",Variant<id:Int32,name:Utf8>)`,
			"Variant<'id':Int32,'name':Utf8>", arrow.DENSE_UNION,
		},
		{
			"VariantNullPayload", `Variant(Nothing(Int32?),"0",Variant<Int32?,Utf8>)`,
			"Variant<Optional<Int32>,Utf8>", arrow.DENSE_UNION,
		},
		{"TaggedNull", `AsTagged(Nothing(Int32?),"tag")`, `Tagged<Optional<Int32>,"tag">`, arrow.INT32},
		{
			"TaggedNestedOptional", `AsTagged(Just(Nothing(Int32?)),"tag")`,
			`Tagged<Optional<Optional<Int32>>,"tag">`, arrow.STRUCT,
		},
		{"NegativeDecimal", `Decimal("-1.23",31,9)`, "Decimal(31,9)", arrow.FIXED_SIZE_BINARY},
		{"DecimalNaN", `Decimal("NaN",35,0)`, "Decimal(35,0)", arrow.FIXED_SIZE_BINARY},
		{"DecimalInf", `Decimal("Inf",35,0)`, "Decimal(35,0)", arrow.FIXED_SIZE_BINARY},

		{"PgNull", `PgCast(NULL,PgInt4)`, "PgType(23)", arrow.STRING},
		{"EmptyList", `[]`, "EmptyList", arrow.STRUCT},
		{"EmptyDict", `AsDict()`, "EmptyDict", arrow.STRUCT},
		{"Tagged", `AsTagged(42,"tag")`, `Tagged<Int32,"tag">`, arrow.INT32},
		{"EscapedTagged", `AsTagged(42,"a'b\\c")`, `Tagged<Int32,"a'b\\c">`, arrow.INT32},
		{"TzDate32", `TzDate32("1969-12-31,Europe/Moscow")`, "TzDate32", arrow.STRUCT},
		{"TzDatetime64", `TzDatetime64("1969-12-31T12:34:56,Europe/Moscow")`, "TzDatetime64", arrow.STRUCT},
		{"TzTimestamp64", `TzTimestamp64("1969-12-31T12:34:56.123456,Europe/Moscow")`, "TzTimestamp64", arrow.STRUCT},
	}
}

func readResultValues(ctx context.Context, t *testing.T, result query.Result, wantRows int) [][][]types.Value {
	t.Helper()
	var values [][][]types.Value
	for rs, err := range result.ResultSets(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		var rows [][]types.Value
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row.Values())
		}
		if len(rows) != wantRows {
			t.Fatalf("baseline rows=%d, want %d", len(rows), wantRows)
		}
		values = append(values, rows)
	}

	return values
}

func assertValueRows(ctx context.Context, t *testing.T, executor query.Executor, statement string, want []types.Value) {
	t.Helper()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	var calls atomic.Int32
	factory := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
		calls.Add(1)

		return ipc.NewReader(part, opts...)
	}
	result, err := executor.QueryResultSet(ctx, statement, query.WithResultFormatArrow(factory, ipc.WithAllocator(alloc)))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close(ctx)
	if columns := result.ColumnTypes(); len(columns) != len(want) {
		t.Fatalf("columns=%d, want %d", len(columns), len(want))
	}
	var count int
	var saved []types.Value
	for row, err := range result.Rows(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if count != 1 {
			t.Fatal("unexpected extra row")
		}
		saved = row.Values()
		assertValues(t, saved, want)
		var scanned types.Value
		if err := row.Scan(&scanned); err != nil {
			t.Fatal(err)
		}
		assertValues(t, []types.Value{scanned}, want)
		if err := row.ScanNamed(query.Named("value", &scanned)); err != nil {
			t.Fatal(err)
		}
		assertValues(t, []types.Value{scanned}, want)
		dst := struct {
			Value types.Value `sql:"value"`
		}{}
		if err := row.ScanStruct(&dst); err != nil {
			t.Fatal(err)
		}
		assertValues(t, []types.Value{dst.Value}, want)
		var expected, actual driver.Value
		expectedErr := types.CastTo(want[0], &expected)
		actualErr := row.Scan(&actual)
		if (expectedErr == nil) != (actualErr == nil) || (expectedErr == nil && !reflect.DeepEqual(actual, expected)) {
			t.Fatalf("driver.Value Scan: got %v (%v), want %v (%v)", actual, actualErr, expected, expectedErr)
		}
	}
	if count != 1 || calls.Load() == 0 {
		t.Fatalf("rows=%d, IPC reader calls=%d", count, calls.Load())
	}
	if err := result.Close(ctx); err != nil {
		t.Fatal(err)
	}
	assertValues(t, saved, want)
}

func assertValues(t *testing.T, got, want []types.Value) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values=%d, want %d", len(got), len(want))
	}
	for i := range got {
		if !proto.Equal(value.ToYDB(got[i]), value.ToYDB(want[i])) || got[i].Yql() != want[i].Yql() {
			t.Fatalf("column %d: got %s (%s), want %s (%s)", i,
				got[i].Yql(), got[i].Type().Yql(), want[i].Yql(), want[i].Type().Yql())
		}
	}
}

func assertWideRow(t *testing.T, row query.Row, want []types.Value) []types.Value {
	t.Helper()
	owned := row.Values()
	assertValues(t, owned, want)
	scanned := make([]types.Value, len(want))
	dst := make([]any, len(want))
	named := make([]query.NamedDestination, len(want))
	fields := make([]reflect.StructField, len(want))
	for i := range scanned {
		dst[i] = &scanned[i]
		named[len(want)-1-i] = query.Named(fmt.Sprintf("v%d", i), &scanned[i])
		fields[i] = reflect.StructField{
			Name: fmt.Sprintf("V%d", i), Type: reflect.TypeFor[types.Value](),
			Tag: reflect.StructTag(fmt.Sprintf(`sql:"v%d"`, i)),
		}
	}
	if err := row.Scan(dst...); err != nil {
		t.Fatal(err)
	}
	assertValues(t, scanned, want)
	if err := row.ScanNamed(named...); err != nil {
		t.Fatal(err)
	}
	assertValues(t, scanned, want)
	structure := reflect.New(reflect.StructOf(fields))
	if err := row.ScanStruct(structure.Interface()); err != nil {
		t.Fatal(err)
	}
	for i := range scanned {
		scanned[i] = structure.Elem().Field(i).Interface().(types.Value)
	}
	assertValues(t, scanned, want)

	return owned
}
