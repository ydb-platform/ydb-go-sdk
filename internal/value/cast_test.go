//go:build go1.18
// +build go1.18

package value

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCastNumbers(t *testing.T) {
	numberValues := []struct {
		value  Value
		signed bool
		len    int
	}{
		{
			value:  Uint64Value(1),
			signed: false,
			len:    8,
		},
		{
			value:  Int64Value(2),
			signed: true,
			len:    8,
		},
		{
			value:  Uint32Value(3),
			signed: false,
			len:    4,
		},
		{
			value:  Int32Value(4),
			signed: true,
			len:    4,
		},
		{
			value:  Uint16Value(5),
			signed: false,
			len:    2,
		},
		{
			value:  Int16Value(6),
			signed: true,
			len:    2,
		},
		{
			value:  Uint8Value(7),
			signed: false,
			len:    1,
		},
		{
			value:  Int8Value(8),
			signed: true,
			len:    1,
		},
	}
	numberDestinations := []struct {
		destination interface{}
		signed      bool
		len         int
	}{
		{
			destination: func(v uint64) *uint64 { return &v }(1),
			signed:      false,
			len:         8,
		},
		{
			destination: func(v int64) *int64 { return &v }(2),
			signed:      true,
			len:         8,
		},
		{
			destination: func(v uint32) *uint32 { return &v }(3),
			signed:      false,
			len:         4,
		},
		{
			destination: func(v int32) *int32 { return &v }(4),
			signed:      true,
			len:         4,
		},
		{
			destination: func(v uint16) *uint16 { return &v }(5),
			signed:      false,
			len:         2,
		},
		{
			destination: func(v int16) *int16 { return &v }(6),
			signed:      true,
			len:         2,
		},
		{
			destination: func(v uint8) *uint8 { return &v }(7),
			signed:      false,
			len:         1,
		},
		{
			destination: func(v int8) *int8 { return &v }(8),
			signed:      true,
			len:         1,
		},
	}
	//nolint:nestif
	for _, src := range numberValues {
		t.Run(src.value.String(), func(t *testing.T) {
			for _, dst := range numberDestinations {
				t.Run(reflect.ValueOf(dst.destination).Type().Elem().String(), func(t *testing.T) {
					t.Run("primitive", func(t *testing.T) {
						mustErr := false
						if src.signed {
							if !dst.signed {
								mustErr = true
							} else if src.len > dst.len {
								mustErr = true
							}
						} else {
							if src.len > dst.len {
								mustErr = true
							} else if src.len == dst.len && dst.signed {
								mustErr = true
							}
						}
						err := CastTo(src.value, dst.destination)
						require.Equal(t, mustErr, (err != nil), err)
					})
					t.Run("optional", func(t *testing.T) {
						mustErr := false
						if src.signed {
							if !dst.signed {
								mustErr = true
							} else if src.len > dst.len {
								mustErr = true
							}
						} else {
							if src.len > dst.len {
								mustErr = true
							} else if src.len == dst.len && dst.signed {
								mustErr = true
							}
						}
						err := CastTo(OptionalValue(src.value), dst.destination)
						require.Equal(t, mustErr, (err != nil), err)
					})
				})
			}
		})
	}
}

func TestCastOtherTypes(t *testing.T) {
	for _, tt := range []struct {
		v      Value
		dst    interface{}
		result interface{}
		error  bool
	}{
		{
			v:      StringValue([]byte("test")),
			dst:    func(v []byte) *[]byte { return &v }(make([]byte, 0, 10)),
			result: func(v []byte) *[]byte { return &v }([]byte("test")),
			error:  false,
		},
		{
			v:      UTF8Value("test"),
			dst:    func(v []byte) *[]byte { return &v }(make([]byte, 0, 10)),
			result: func(v []byte) *[]byte { return &v }([]byte("test")),
			error:  false,
		},
		{
			v:      StringValue([]byte("test")),
			dst:    func(v string) *string { return &v }(""),
			result: func(v string) *string { return &v }("test"),
			error:  false,
		},
		{
			v:      DoubleValue(123),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
		{
			v:      DoubleValue(123),
			dst:    func(v float32) *float32 { return &v }(9),
			result: func(v float32) *float32 { return &v }(9),
			error:  true,
		},
		{
			v:      FloatValue(123),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
		{
			v:      FloatValue(123),
			dst:    func(v float32) *float32 { return &v }(9),
			result: func(v float32) *float32 { return &v }(123),
			error:  false,
		},
		{
			v:      Uint64Value(123),
			dst:    func(v float32) *float32 { return &v }(9),
			result: func(v float32) *float32 { return &v }(9),
			error:  true,
		},
		{
			v:      Uint64Value(123),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(9),
			error:  true,
		},
		{
			v:      OptionalValue(DoubleValue(123)),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
	} {
		t.Run(fmt.Sprintf("cast %s to %v", tt.v.Type().String(), reflect.ValueOf(tt.dst).Type().Elem()),
			func(t *testing.T) {
				t.Run("primitive", func(t *testing.T) {
					if err := CastTo(tt.v, tt.dst); (err != nil) != tt.error {
						t.Errorf("CastTo() error = %v, want %v", err, tt.error)
					} else if !reflect.DeepEqual(tt.dst, tt.result) {
						t.Errorf("CastTo() result = %v, want %v", tt.dst, tt.result)
					}
				})
				t.Run("optional", func(t *testing.T) {
					if err := CastTo(OptionalValue(tt.v), tt.dst); (err != nil) != tt.error {
						t.Errorf("CastTo() error = %v, want %v", err, tt.error)
					} else if !reflect.DeepEqual(tt.dst, tt.result) {
						t.Errorf("CastTo() result = %v, want %v", tt.dst, tt.result)
					}
				})
			},
		)
	}
}
