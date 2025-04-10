package testutil

import (
	"errors"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestUnwrapOptionalValue(t *testing.T) {
	v := value.OptionalValue(value.OptionalValue(value.TextValue("a")))
	val := unwrapTypedValue(value.ToYDB(v))
	typeID := val.GetType().GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	textValue := val.GetValue().GetValue().(*Ydb.Value_TextValue)
	if text := textValue.TextValue; text != "a" {
		t.Errorf("Values are different: expected %q, actual %q", "a", text)
	}
}

func TestUnwrapPrimitiveValue(t *testing.T) {
	v := value.TextValue("a")
	val := unwrapTypedValue(value.ToYDB(v))
	typeID := val.GetType().GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	textValue := val.GetValue().GetValue().(*Ydb.Value_TextValue)
	if text := textValue.TextValue; text != "a" {
		t.Errorf("Values are different: expected %q, actual %q", "a", text)
	}
}

func TestUnwrapNullValue(t *testing.T) {
	v := value.NullValue(types.Text)
	val := unwrapTypedValue(value.ToYDB(v))
	typeID := val.GetType().GetTypeId()
	if typeID != Ydb.Type_UTF8 {
		t.Errorf("Types are different: expected %d, actual %d", Ydb.Type_UTF8, typeID)
	}
	nullFlagValue := val.GetValue().GetValue().(*Ydb.Value_NullFlagValue)
	if nullFlagValue.NullFlagValue != structpb.NullValue_NULL_VALUE {
		t.Errorf("Values are different: expected %d, actual %d", structpb.NullValue_NULL_VALUE, nullFlagValue.NullFlagValue)
	}
}

func TestUint8(t *testing.T) {
	l := value.Uint8Value(byte(1))
	r := value.Uint8Value(byte(10))
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
	l := value.Int8Value(int8(1))
	r := value.Int8Value(int8(10))
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
	l := value.TimestampValue(1)
	r := value.TimestampValue(10)
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
	l := value.DatetimeValue(1)
	r := value.DatetimeValue(10)
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
	l := value.Uint64Value(uint64(1))
	r := value.Uint64Value(uint64(10))
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
	l := value.Int64Value(int64(1))
	r := value.Int64Value(int64(10))
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
	l := value.DoubleValue(1.0)
	r := value.DoubleValue(2.0)
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
	l := value.FloatValue(1.0)
	r := value.FloatValue(2.0)
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
	l := value.TextValue("abc")
	r := value.TextValue("abx")
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
	l := value.OptionalValue(value.OptionalValue(value.TextValue("abc")))
	r := value.TextValue("abx")
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
	l := value.BytesValue([]byte{1, 2, 3})
	r := value.BytesValue([]byte{1, 2, 5})
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
	l := value.NullValue(types.Text)
	r := value.TextValue("abc")

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
	withNull := value.TupleValue(value.Uint64Value(1), value.NullValue(types.Text))
	least := value.TupleValue(value.Uint64Value(1), value.TextValue("abc"))
	medium := value.TupleValue(value.Uint64Value(1), value.TextValue("def"))
	largest := value.TupleValue(value.Uint64Value(2), value.TextValue("abc"))

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
	least := value.ListValue(value.Uint64Value(1), value.Uint64Value(1))
	medium := value.ListValue(value.Uint64Value(1), value.Uint64Value(2))
	largest := value.ListValue(value.Uint64Value(2), value.Uint64Value(1))

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
	l := value.DyNumberValue("2")
	r := value.DyNumberValue("12")
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
	l := value.UUIDWithIssue1501Value([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	r := value.UUIDWithIssue1501Value([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
	g := value.UUIDWithIssue1501Value([16]byte{100, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
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
	l := value.Uint64Value(1)
	r := value.TimestampValue(2)
	_, err := Compare(l, r)
	if err == nil {
		t.Errorf("WithStackTrace expected")
	}
	if !errors.Is(err, ErrNotComparable) {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestIncompatibleTuples(t *testing.T) {
	l := value.TupleValue(value.Uint64Value(1), value.TextValue("abc"))
	r := value.TupleValue(value.Uint64Value(1), value.BytesValue([]byte("abc")))
	if _, err := Compare(l, r); err == nil {
		t.Error("WithStackTrace expected")
	} else if !errors.Is(err, ErrNotComparable) {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestTupleOfDifferentLength(t *testing.T) {
	l := value.TupleValue(value.Uint64Value(1), value.TextValue("abc"))
	r := value.TupleValue(value.Uint64Value(1), value.TextValue("abc"), value.TextValue("def"))

	cmp, err := Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)
}

func TestTupleInTuple(t *testing.T) {
	l := value.TupleValue(value.Uint64Value(1), value.TupleValue(value.TextValue("abc"), value.BytesValue([]byte("xyz"))))
	r := value.TupleValue(value.Uint64Value(1), value.TupleValue(value.TextValue("def"), value.BytesValue([]byte("xyz"))))

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
	l := value.ListValue(
		value.ListValue(
			value.TextValue("abc"), value.TextValue("def"),
		), value.ListValue(
			value.TextValue("uvw"), value.TextValue("xyz"),
		),
	)
	r := value.ListValue(
		value.ListValue(
			value.TextValue("abc"), value.TextValue("deg"),
		), value.ListValue(
			value.TextValue("uvw"), value.TextValue("xyz"),
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
