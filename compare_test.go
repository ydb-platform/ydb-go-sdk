package ydb

import (
	"testing"
)

func TestUint8(t *testing.T) {
	l := Uint8Value(byte(1))
	r := Uint8Value(byte(10))
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
	l := Int8Value(int8(1))
	r := Int8Value(int8(10))
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
	l := Uint64Value(uint64(1))
	r := Uint64Value(uint64(10))
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
	l := Int64Value(int64(1))
	r := Int64Value(int64(10))
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
	l := UTF8Value("abc")
	r := UTF8Value("abx")
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

func TestString(t *testing.T) {
	l := StringValue([]byte{1,2,3})
	r := StringValue([]byte{1,2,5})
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
