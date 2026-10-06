package arrow

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestNewDecoderOwnership(t *testing.T) {
	for _, test := range []struct {
		name      string
		column    Column
		count     int64
		fail      error
		cancel    bool
		wantError bool
	}{
		{name: "success", column: Column{Name: "value", Type: types.Int32}, count: 1},
		{name: "column count", column: Column{Name: "value", Type: types.Int32}, count: 2, wantError: true},
		{name: "column name", column: Column{Name: "other", Type: types.Int32}, count: 1, wantError: true},
		{name: "column type", column: Column{Name: "value", Type: types.Text}, count: 1, wantError: true},
		{
			name: "read error", column: Column{Name: "value", Type: types.Int32}, count: 1,
			fail: io.ErrUnexpectedEOF, wantError: true,
		},
		{
			name: "cancel between records", column: Column{Name: "value", Type: types.Int32}, count: 1,
			cancel: true, wantError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			records := []*testRecord{
				{count: 1, data: &testArray{values: []int32{42}}},
				{count: test.count, data: &testArray{values: []int32{43}}},
			}
			reader := &testReader{records: records, fail: test.fail}
			if test.cancel {
				reader.afterRead = cancel
			}
			decode := NewDecoder(func(io.Reader, ...int) (*testReader, error) { return reader, nil })
			// Failures on the second record must also release the first retained record.
			columns := []Column{{Name: "value", Type: types.Int32}}
			reader.beforeRead = func(i int) {
				if i == 1 {
					columns[0] = test.column
				}
			}
			batches, err := decode(ctx, columns, strings.NewReader(""))
			if (err != nil) != test.wantError {
				t.Fatalf("decode error=%v", err)
			}
			if test.fail != nil && !errors.Is(err, test.fail) {
				t.Fatalf("lost error: %v", err)
			}
			if test.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if reader.released != 1 {
				t.Fatalf("reader released %d times", reader.released)
			}
			if test.wantError {
				if len(batches) != 0 {
					t.Fatal("returned partial batches on error")
				}
			}
			if !test.wantError {
				if len(batches) != 2 {
					t.Fatalf("batches=%d", len(batches))
				}
				for i, batch := range batches {
					if records[i].refs != 1 {
						t.Fatal("batch lost its retained record")
					}
					require.Equal(t, 1, batch.NumRows())
					require.Equal(t, 1, batch.NumCols())
					require.Equal(t, value.Int32Value(int32(42+i)), batch.Value(0, 0))
					var got int32
					if err := batch.Scan(0, 0, &got); err != nil || got != int32(42+i) {
						t.Fatalf("scan=%d: %v", got, err)
					}
					batch.Release()
					batch.Release()
				}
			}
			for _, record := range records {
				if record.refs != 0 {
					t.Fatalf("leaked references: %d", record.refs)
				}
			}
		})
	}
}

func TestNewDecoderOptions(t *testing.T) {
	opts := []int{42}
	calls := 0
	decode := NewDecoder(func(_ io.Reader, options ...int) (*testReader, error) {
		calls++
		if len(options) != 1 || options[0] != 42 {
			t.Fatalf("options=%v", options)
		}

		return &testReader{}, nil
	}, opts...)
	opts[0] = 43
	for range 2 {
		batches, err := decode(t.Context(), nil, strings.NewReader(""))
		if err != nil || len(batches) != 0 {
			t.Fatalf("empty IPC: %v, %v", batches, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := decode(ctx, nil, strings.NewReader("")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if calls != 2 {
		t.Fatalf("factory calls=%d", calls)
	}
}

func TestNewDecoderFactoryError(t *testing.T) {
	expected := errors.New("factory error")
	decode := NewDecoder(func(io.Reader, ...int) (*testReader, error) { return nil, expected })
	if batches, err := decode(t.Context(), nil, strings.NewReader("")); !errors.Is(err, expected) || len(batches) != 0 {
		t.Fatalf("factory: %v, %v", batches, err)
	}
}

func TestNewDecoderScalarTypes(t *testing.T) {
	for _, test := range []scalarTestCase{
		newScalarTestCase(true, value.BoolValue),
		newScalarTestCase(false, value.BoolValue),
		newScalarTestCase(int8(-7), value.Int8Value),
		newScalarTestCase(int16(-700), value.Int16Value),
		newScalarTestCase(int32(-70000), value.Int32Value),
		newScalarTestCase(int64(-7000000000), value.Int64Value),
		newScalarTestCase(uint8(7), value.Uint8Value),
		newScalarTestCase(uint16(700), value.Uint16Value),
		newScalarTestCase(uint32(70000), value.Uint32Value),
		newScalarTestCase(uint64(7000000000), value.Uint64Value),
		newScalarTestCase(float32(1.25), value.FloatValue),
		newScalarTestCase(float64(2.5), value.DoubleValue),
		newScalarTestCase("text", value.TextValue),
		newScalarTestCase([]byte("bytes"), value.BytesValue),
	} {
		t.Run(test.want.Yql(), func(t *testing.T) {
			for _, mode := range []string{"required", "optional", "nullable"} {
				t.Run(mode, func(t *testing.T) {
					typ := test.want.Type()
					if mode != "required" {
						typ = types.NewOptional(typ)
					}
					record := &testRecord{count: 1, data: test.array(mode == "nullable")}
					reader := &testReader{records: []*testRecord{record}}
					decode := NewDecoder(func(io.Reader, ...int) (*testReader, error) { return reader, nil })
					batches, err := decode(t.Context(), []Column{{Name: "value", Type: typ}}, strings.NewReader(""))
					require.NoError(t, err)
					require.Len(t, batches, 1)
					batch := batches[0]
					t.Cleanup(batch.Release)
					require.Equal(t, 2, batch.NumRows())
					require.Equal(t, 1, batch.NumCols())
					ptr := reflect.New(reflect.PointerTo(reflect.TypeOf(test.native)))
					for row := range 2 {
						want := test.want
						if mode != "required" {
							want = value.OptionalValue(want)
						}
						null := mode == "nullable" && row == 1
						if null {
							want = value.NullValue(test.want.Type())
						}
						require.Equal(t, want, batch.Value(row, 0))
						dst := reflect.New(reflect.TypeOf(test.native))
						if null {
							expected := reflect.New(dst.Elem().Type())
							require.NoError(t, value.CastTo(want, expected.Interface()))
							require.NoError(t, batch.Scan(row, 0, dst.Interface()))
							require.Equal(t, expected.Elem().Interface(), dst.Elem().Interface())
						} else {
							require.NoError(t, batch.Scan(row, 0, dst.Interface()))
							require.Equal(t, test.native, dst.Elem().Interface())
						}
						if mode == "required" {
							require.ErrorIs(t, batch.Scan(row, 0, ptr.Interface()), value.ErrCannotCast)
						} else {
							require.NoError(t, batch.Scan(row, 0, ptr.Interface()))
							if null {
								require.True(t, ptr.Elem().IsNil())
							} else {
								require.Equal(t, test.native, ptr.Elem().Elem().Interface())
							}
						}
						var expected, actual driver.Value
						require.NoError(t, value.CastTo(want, &expected))
						require.NoError(t, batch.Scan(row, 0, &actual))
						require.Equal(t, expected, actual)
						require.Error(t, batch.Scan(row, 0, struct{}{}))
					}
					batch.Release()
					require.Zero(t, record.refs)
				})
			}
		})
	}
}

func TestNewDecoderOwnsVariableWidthValues(t *testing.T) {
	for _, optional := range []bool{false, true} {
		for _, text := range []bool{false, true} {
			buffer := []byte("owned data")
			test := newScalarTestCase(buffer, value.BytesValue)
			want := value.Value(value.BytesValue([]byte("owned data")))
			if text {
				test = newScalarTestCase(unsafe.String(unsafe.SliceData(buffer), len(buffer)), value.TextValue)
				want = value.TextValue("owned data")
			}
			t.Run(fmt.Sprintf("%s/optional=%t", want.Type(), optional), func(t *testing.T) {
				typ := want.Type()
				if optional {
					typ = types.NewOptional(typ)
					want = value.OptionalValue(want)
				}
				record := &testRecord{count: 1, data: test.array(false)}
				decode := NewDecoder(func(io.Reader, ...int) (*testReader, error) {
					return &testReader{records: []*testRecord{record}}, nil
				})
				batches, err := decode(t.Context(), []Column{{Name: "value", Type: typ}}, strings.NewReader(""))
				require.NoError(t, err)
				require.Len(t, batches, 1)
				batch := batches[0]
				t.Cleanup(batch.Release)
				owned := batch.Value(0, 0)
				dst := reflect.New(reflect.TypeOf(test.native))
				require.NoError(t, batch.Scan(0, 0, dst.Interface()))
				ptr := reflect.New(dst.Type())
				if optional {
					require.NoError(t, batch.Scan(0, 0, ptr.Interface()))
				}
				var fallback, expected driver.Value
				require.NoError(t, batch.Scan(0, 0, &fallback))
				require.NoError(t, value.CastTo(want, &expected))
				batch.Release()
				clear(buffer)
				require.Equal(t, want, owned)
				require.Equal(t, expected, dst.Elem().Interface())
				require.Equal(t, expected, fallback)
				if optional {
					require.Equal(t, expected, ptr.Elem().Elem().Interface())
				}
			})
		}
	}
}

func TestNewColumnBoolAndInvalidTypes(t *testing.T) {
	data := &scalarTestArray[uint8]{values: []uint8{0, 1, 2}, nulls: []bool{false, false, true}}
	column, err := newColumn(data, types.NewOptional(types.Bool))
	require.NoError(t, err)
	for row, want := range []value.Value{
		value.OptionalValue(value.BoolValue(false)),
		value.OptionalValue(value.BoolValue(true)),
		value.NullValue(types.Bool),
	} {
		require.Equal(t, want, column.value(row))
		var expected, actual *bool
		require.NoError(t, value.CastTo(want, &expected))
		require.NoError(t, column.scan(row, &actual))
		require.Equal(t, expected, actual)
	}
	data.nulls = nil
	column, err = newColumn(data, types.Bool)
	require.NoError(t, err)
	var nonzero bool
	require.NoError(t, column.scan(2, &nonzero))
	require.True(t, nonzero)
	for _, test := range []struct {
		array Array
		typ   types.Type
		want  string
	}{
		{&scalarTestArray[int32]{values: []int32{0}, nulls: []bool{true}}, types.Int32, "null in non-optional"},
		{&scalarTestArray[int32]{values: []int32{0}}, types.Text, "does not match YDB"},
		{&scalarTestArray[uint32]{values: []uint32{0}}, types.Date, "does not match YDB"},
		{&scalarTestArray[uint32]{values: []uint32{0}}, types.Datetime64, "does not match YDB"},
		{&scalarTestArray[uint64]{values: []uint64{0}}, types.Timestamp64, "does not match YDB"},
		{&scalarTestArray[uint64]{values: []uint64{0}}, types.Interval, "does not match YDB"},
		{
			&scalarTestArray[int32]{values: []int32{0}},
			types.NewOptional(types.NewOptional(types.Int32)), "expected Arrow optional wrapper",
		},
		{&scalarTestArray[struct{}]{values: []struct{}{{}}}, types.Int32, "unsupported Arrow array"},
	} {
		t.Run(test.want+"/"+test.typ.Yql(), func(t *testing.T) {
			_, err := newColumn(test.array, test.typ)
			require.ErrorContains(t, err, test.want)
		})
	}
}

type testArray struct{ values []int32 }

func (a *testArray) Len() int          { return len(a.values) }
func (a *testArray) IsNull(int) bool   { return false }
func (a *testArray) NullN() int        { return 0 }
func (a *testArray) Value(i int) int32 { return a.values[i] }

type testRecord struct {
	data  Array
	count int64
	refs  int
}

func (r *testRecord) Retain()               { r.refs++ }
func (r *testRecord) Release()              { r.refs-- }
func (r *testRecord) NumRows() int64        { return int64(r.data.Len()) }
func (r *testRecord) NumCols() int64        { return r.count }
func (r *testRecord) ColumnName(int) string { return "value" }
func (r *testRecord) Column(int) Array      { return r.data }

type testReader struct {
	records    []*testRecord
	current    *testRecord
	index      int
	released   int
	fail       error
	beforeRead func(int)
	afterRead  func()
}

func (r *testReader) Read() (*testRecord, error) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.index == len(r.records) {
		if r.fail != nil {
			return nil, r.fail
		}

		return nil, io.EOF
	}
	if r.beforeRead != nil {
		r.beforeRead(r.index)
	}
	r.current = r.records[r.index]
	r.current.Retain()
	r.index++
	if r.afterRead != nil {
		r.afterRead()
	}

	return r.current, nil
}

func (r *testReader) Release() {
	r.released++
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

type scalarTestCase struct {
	array  func(nullable bool) Array
	native any
	want   value.Value
}

func newScalarTestCase[T any, V value.Value](native T, makeValue func(T) V) scalarTestCase {
	return scalarTestCase{
		native: native,
		want:   makeValue(native),
		array: func(nullable bool) Array {
			return &scalarTestArray[T]{values: []T{native, native}, nulls: []bool{false, nullable}}
		},
	}
}

type scalarTestArray[T any] struct {
	values []T
	nulls  []bool
}

func (a *scalarTestArray[T]) Len() int            { return len(a.values) }
func (a *scalarTestArray[T]) IsNull(row int) bool { return len(a.nulls) != 0 && a.nulls[row] }
func (a *scalarTestArray[T]) NullN() int {
	n := 0
	for _, null := range a.nulls {
		if null {
			n++
		}
	}

	return n
}
func (a *scalarTestArray[T]) Value(row int) T { return a.values[row] }
