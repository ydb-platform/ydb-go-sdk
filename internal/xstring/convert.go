//go:build !go1.21
// +build !go1.21

package xstring

import (
	"reflect"
	"unsafe"
)

func FromBytes(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func ToBytes(s string) (b []byte) {
	pb := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	ps := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pb.Data = ps.Data
	pb.Len = ps.Len
	pb.Cap = ps.Len

	return b
}
