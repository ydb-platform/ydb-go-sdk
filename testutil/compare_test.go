package testutil

import (
	"errors"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestUnwrapOptionalValue(t *testing.T) {
	a := allocator.New()
	defer a.Free()
	v := value.OptionalValue(value.OptionalValue(value.TextValue("a")))
	val := unwrapTypedValue(value.ToYDB(v, a))
	typeID := val.Type.GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	textValue := val.Value.Value.(*Ydb.Value_TextValue)
	text := textValue.TextValue
	if text != "a" {
		t.Errorf("Values are different: expected %q, actual %q", "a", text)
	}
}

func TestUnwrapPrimitiveValue(t *testing.T) {
	a := allocator.New()
	defer a.Free()
	v := value.TextValue("a")
	val := unwrapTypedValue(value.ToYDB(v, a))
	typeID := val.Type.GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	textValue := val.Value.Value.(*Ydb.Value_TextValue)
	text := textValue.TextValue
	if text != "a" {
		t.Errorf("Values are different: expected %q, actual %q", "a", text)
	}
}

func TestUnwrapNullValue(t *testing.T) {
	a := allocator.New()
	defer a.Free()
	v := value.NullValue(value.TypeText)
	val := unwrapTypedValue(value.ToYDB(v, a))
	typeID := val.Type.GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	nullFlagValue := val.Value.Value.(*Ydb.Value_NullFlagValue)
	if nullFlagValue.NullFlagValue != structpb.NullValue_NULL_VALUE {
		t.Errorf("Values are different: expected %d, actual %d", structpb.NullValue_NULL_VALUE, nullFlagValue.NullFlagValue)
	}
}

func TestUint8(t *testing.T) {
	l := types.Uint8Value(byte(1))
	r := types.Uint8Value(byte(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestInt8(t *testing.T) {
	l := types.Int8Value(int8(1))
	r := types.Int8Value(int8(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestTimestamp(t *testing.T) {
	l := types.TimestampValue(1)
	r := types.TimestampValue(10)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestDateTime(t *testing.T) {
	l := types.DatetimeValue(1)
	r := types.DatetimeValue(10)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUint64(t *testing.T) {
	l := types.Uint64Value(uint64(1))
	r := types.Uint64Value(uint64(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestInt64(t *testing.T) {
	l := types.Int64Value(int64(1))
	r := types.Int64Value(int64(10))
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestDouble(t *testing.T) {
	l := types.DoubleValue(1.0)
	r := types.DoubleValue(2.0)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestFloat(t *testing.T) {
	l := types.FloatValue(1.0)
	r := types.FloatValue(2.0)
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUTF8(t *testing.T) {
	l := types.TextValue("abc")
	r := types.TextValue("abx")
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestOptionalUTF8(t *testing.T) {
	l := types.OptionalValue(types.OptionalValue(types.TextValue("abc")))
	r := types.TextValue("abx")
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestBytes(t *testing.T) {
	l := types.BytesValue([]byte{1, 2, 3})
	r := types.BytesValue([]byte{1, 2, 5})
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestNull(t *testing.T) {
	l := types.NullValue(types.TypeText)
	r := types.TextValue("abc")

	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestTuple(t *testing.T) {
	withNull := types.TupleValue(types.Uint64Value(1), types.NullValue(types.TypeText))
	least := types.TupleValue(types.Uint64Value(1), types.TextValue("abc"))
	medium := types.TupleValue(types.Uint64Value(1), types.TextValue("def"))
	largest := types.TupleValue(types.Uint64Value(2), types.TextValue("abc"))

	c, err := Compare(least, medium)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(least, largest)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(largest, medium)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(withNull, least)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(withNull, withNull)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestList(t *testing.T) {
	least := types.ListValue(types.Uint64Value(1), types.Uint64Value(1))
	medium := types.ListValue(types.Uint64Value(1), types.Uint64Value(2))
	largest := types.ListValue(types.Uint64Value(2), types.Uint64Value(1))

	c, err := Compare(least, medium)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(least, largest)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(largest, medium)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)
}

func TestDyNumber(t *testing.T) {
	l := types.DyNumberValue("2")
	r := types.DyNumberValue("12")
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUUID(t *testing.T) {
	l := types.UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	r := types.UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
	g := types.UUIDValue([16]byte{100, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
	c, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(l, g)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestIncompatiblePrimitives(t *testing.T) {
	l := types.Uint64Value(1)
	r := types.TimestampValue(2)
	_, err := Compare(l, r)
	if err == nil {
		t.Errorf("WithStackTrace expected")
	}
	if !errors.Is(err, ErrNotComparable) {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestIncompatibleTuples(t *testing.T) {
	l := types.TupleValue(types.Uint64Value(1), types.TextValue("abc"))
	r := types.TupleValue(types.Uint64Value(1), types.BytesValue([]byte("abc")))
	_, err := Compare(l, r)
	if err == nil {
		t.Error("WithStackTrace expected")
	} else if !errors.Is(err, ErrNotComparable) {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestTupleOfDifferentLength(t *testing.T) {
	l := types.TupleValue(types.Uint64Value(1), types.TextValue("abc"))
	r := types.TupleValue(types.Uint64Value(1), types.TextValue("abc"), types.TextValue("def"))

	cmp, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)
}

func TestTupleInTuple(t *testing.T) {
	l := types.TupleValue(types.Uint64Value(1), types.TupleValue(types.TextValue("abc"), types.BytesValue([]byte("xyz"))))
	r := types.TupleValue(types.Uint64Value(1), types.TupleValue(types.TextValue("def"), types.BytesValue([]byte("xyz"))))

	cmp, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)

	cmp, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, cmp)
}

func TestListInList(t *testing.T) {
	l := types.ListValue(
		types.ListValue(
			types.TextValue("abc"), types.TextValue("def"),
		), types.ListValue(
			types.TextValue("uvw"), types.TextValue("xyz"),
		),
	)
	r := types.ListValue(
		types.ListValue(
			types.TextValue("abc"), types.TextValue("deg"),
		), types.ListValue(
			types.TextValue("uvw"), types.TextValue("xyz"),
		),
	)

	cmp, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)

	cmp, err = Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, cmp)
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
}

func requireEqualValues(t *testing.T, expected int, actual int) {
	t.Helper()
	if expected != actual {
		t.Errorf("Values not equal: expected %v, actual %v", expected, actual)
	}
}
