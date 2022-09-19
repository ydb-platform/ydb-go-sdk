//go:build go1.18
// +build go1.18

package value

import (
	"fmt"
	"reflect"
	"testing"
)

func makePointerType[T any](dst T) *T {
	return &dst
}

func TestCastTo(t *testing.T) {
	for _, tt := range []struct {
		v      Value
		dst    interface{}
		result interface{}
		error  bool
	}{
		{
			v:      StringValue([]byte("test")),
			dst:    makePointerType(make([]byte, 0, 10)),
			result: makePointerType([]byte("test")),
			error:  false,
		},
		{
			v:      UTF8Value("test"),
			dst:    makePointerType(make([]byte, 0, 10)),
			result: makePointerType([]byte("test")),
			error:  false,
		},
		{
			v:      StringValue([]byte("test")),
			dst:    makePointerType(""),
			result: makePointerType("test"),
			error:  false,
		},
		{
			v:      UTF8Value("test"),
			dst:    makePointerType(""),
			result: makePointerType("test"),
			error:  false,
		},
		{
			v:      Uint64Value(100500),
			dst:    makePointerType(uint64(100500)),
			result: makePointerType(uint64(100500)),
			error:  false,
		},
		{
			v:      Int64Value(100500),
			dst:    makePointerType(int64(100500)),
			result: makePointerType(int64(100500)),
			error:  false,
		},
		{
			v:      Uint32Value(100500),
			dst:    makePointerType(uint32(100500)),
			result: makePointerType(uint32(100500)),
			error:  false,
		},
		{
			v:      Int32Value(100500),
			dst:    makePointerType(int32(100500)),
			result: makePointerType(int32(100500)),
			error:  false,
		},
		{
			v:      Uint16Value(123),
			dst:    makePointerType(uint16(123)),
			result: makePointerType(uint16(123)),
			error:  false,
		},
		{
			v:      Int16Value(123),
			dst:    makePointerType(int16(123)),
			result: makePointerType(int16(123)),
			error:  false,
		},
		{
			v:      Uint8Value(123),
			dst:    makePointerType(uint8(123)),
			result: makePointerType(uint8(123)),
			error:  false,
		},
		{
			v:      Int8Value(123),
			dst:    makePointerType(int8(123)),
			result: makePointerType(int8(123)),
			error:  false,
		},
		{
			v:      DoubleValue(123),
			dst:    makePointerType(float64(123)),
			result: makePointerType(float64(123)),
			error:  false,
		},
		{
			v:      FloatValue(123),
			dst:    makePointerType(float32(123)),
			result: makePointerType(float32(123)),
			error:  false,
		},
	} {
		t.Run(fmt.Sprintf("cast %s to %v", tt.v.Type().String(), reflect.ValueOf(tt.dst).Type().Elem()), func(t *testing.T) {
			if err := CastTo(tt.v, tt.dst); (err != nil) != tt.error {
				t.Errorf("CastTo() error = %v, want %v", err, tt.error)
			} else if !reflect.DeepEqual(tt.dst, tt.result) {
				t.Errorf("CastTo() result = %v, want %v", tt.dst, tt.result)
			}
		})
	}
}
