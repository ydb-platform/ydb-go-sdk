//go:build go1.20
// +build go1.20

package xstring

import (
	"unsafe"
)

func FromBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func ToBytes(s string) (b []byte) {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
