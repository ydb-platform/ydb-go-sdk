package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"strings"
	"testing"
)

func TestUint8(t *testing.T) {
	l := types.Uint8Value(byte(1))
	r := types.Uint8Value(byte(10))
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestInt8(t *testing.T) {
	l := types.Int8Value(int8(1))
	r := types.Int8Value(int8(10))
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestTimestamp(t *testing.T) {
	l := types.TimestampValue(1)
	r := types.TimestampValue(10)
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestDateTime(t *testing.T) {
	l := types.DatetimeValue(1)
	r := types.DatetimeValue(10)
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUint64(t *testing.T) {
	l := types.Uint64Value(uint64(1))
	r := types.Uint64Value(uint64(10))
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestInt64(t *testing.T) {
	l := types.Int64Value(int64(1))
	r := types.Int64Value(int64(10))
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestDouble(t *testing.T) {
	l := types.DoubleValue(1.0)
	r := types.DoubleValue(2.0)
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestFloat(t *testing.T) {
	l := types.FloatValue(1.0)
	r := types.FloatValue(2.0)
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUTF8(t *testing.T) {
	l := types.UTF8Value("abc")
	r := types.UTF8Value("abx")
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestOptionalUTF8(t *testing.T) {
	l := types.OptionalValue(types.OptionalValue(types.UTF8Value("abc")))
	r := types.UTF8Value("abx")
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestString(t *testing.T) {
	l := types.StringValue([]byte{1, 2, 3})
	r := types.StringValue([]byte{1, 2, 5})
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestNull(t *testing.T) {
	l := types.NullValue(types.TypeUTF8)
	r := types.UTF8Value("abc")
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestTuple(t *testing.T) {
	withNull := types.TupleValue(types.Uint64Value(1), types.NullValue(types.TypeUTF8))
	least := types.TupleValue(types.Uint64Value(1), types.UTF8Value("abc"))
	medium := types.TupleValue(types.Uint64Value(1), types.UTF8Value("def"))
	largest := types.TupleValue(types.Uint64Value(2), types.UTF8Value("abc"))

	c, err := types.Compare(least, medium)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(least, largest)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(largest, medium)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(withNull, least)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(withNull, withNull)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)

}

func TestList(t *testing.T) {
	least := types.ListValue(types.Uint64Value(1), types.Uint64Value(1))
	medium := types.ListValue(types.Uint64Value(1), types.Uint64Value(2))
	largest := types.ListValue(types.Uint64Value(2), types.Uint64Value(1))

	c, err := types.Compare(least, medium)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(least, largest)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(largest, medium)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

}

func TestDyNumber(t *testing.T) {
	l := types.DyNumberValue("2")
	r := types.DyNumberValue("12")
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestUUID(t *testing.T) {
	l := types.UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	r := types.UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
	g := types.UUIDValue([16]byte{100, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17})
	c, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(l, g)
	requireNoError(t, err)
	requireEqualValues(t, -1, c)

	c, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, c)

	c, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, c)
}

func TestIncompatiblePrimitives(t *testing.T) {
	l := types.Uint64Value(1)
	r := types.TimestampValue(2)
	_, err := types.Compare(l, r)
	if err == nil {
		t.Errorf("Error expected")
	}
	if !strings.HasPrefix(err.Error(), "not comparable:") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestIncompatibleTuples(t *testing.T) {
	l := types.TupleValue(types.Uint64Value(1), types.UTF8Value("abc"))
	r := types.TupleValue(types.Uint64Value(1), types.StringValue([]byte("abc")))
	_, err := types.Compare(l, r)
	if err == nil {
		t.Error("Error expected")
	} else {
		if !strings.HasPrefix(err.Error(), "not comparable:") {
			t.Errorf("Unexpected error message: %s", err.Error())
		}
	}
}

func TestTupleOfDifferentLength(t *testing.T) {
	l := types.TupleValue(types.Uint64Value(1), types.UTF8Value("abc"))
	r := types.TupleValue(types.Uint64Value(1), types.UTF8Value("abc"), types.UTF8Value("def"))

	cmp, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)
}

func TestTupleInTuple(t *testing.T) {
	l := types.TupleValue(types.Uint64Value(1), types.TupleValue(types.UTF8Value("abc"), types.StringValue([]byte("xyz"))))
	r := types.TupleValue(types.Uint64Value(1), types.TupleValue(types.UTF8Value("def"), types.StringValue([]byte("xyz"))))

	cmp, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)

	cmp, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, cmp)
}

func TestListInList(t *testing.T) {
	l := types.ListValue(types.ListValue(types.UTF8Value("abc"), types.UTF8Value("def")), types.ListValue(types.UTF8Value("uvw"), types.UTF8Value("xyz")))
	r := types.ListValue(types.ListValue(types.UTF8Value("abc"), types.UTF8Value("deg")), types.ListValue(types.UTF8Value("uvw"), types.UTF8Value("xyz")))

	cmp, err := types.Compare(l, r)
	requireNoError(t, err)
	requireEqualValues(t, -1, cmp)

	cmp, err = types.Compare(r, l)
	requireNoError(t, err)
	requireEqualValues(t, 1, cmp)

	cmp, err = types.Compare(l, l)
	requireNoError(t, err)
	requireEqualValues(t, 0, cmp)
}

func requireNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
}

func requireEqualValues(t *testing.T, expected int, actual int) {
	if expected != actual {
		t.Errorf("Values not equal: expected %v, actual %v", expected, actual)
	}
}
