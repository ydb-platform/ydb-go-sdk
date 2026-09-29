package sugar

import (
	"encoding/binary"
	"math"
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

type (
	signed interface {
		~int | ~int8 | ~int16 | ~int32 | ~int64
	}
	unsigned interface {
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
	}
	integer interface {
		signed | unsigned
	}
	float interface {
		~float32 | ~float64
	}
	number interface {
		integer | float
	}
)

func endByte(kind reflect.Kind) byte {
	switch kind {
	case reflect.Int8:
		return 0x03
	case reflect.Uint8:
		return 0x02
	default:
		return 0x01
	}
}

// Embedding returns a Bytes value containing the given numbers in YDB KNN format.
// int8 and uint8 values use their one-byte formats; other numbers are converted to float32.
// With no numbers, it returns a Bytes value containing only the type marker.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func Embedding[T number](embedding ...T) types.Value {
	switch kind := reflect.TypeFor[T]().Kind(); kind {
	case reflect.Int8, reflect.Uint8:
		bytes := make([]byte, len(embedding)+1)
		for i, value := range embedding {
			bytes[i] = byte(value)
		}
		bytes[len(bytes)-1] = endByte(kind)

		return types.BytesValue(bytes)
	default:
		bytes := make([]byte, len(embedding)*4+1)
		for i, value := range embedding {
			binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(float32(value)))
		}
		bytes[len(bytes)-1] = endByte(kind)

		return types.BytesValue(bytes)
	}
}
