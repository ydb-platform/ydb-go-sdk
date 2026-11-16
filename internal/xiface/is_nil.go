package xiface

import "unsafe"

// emptyInterface mirrors the runtime representation of an interface value.
// It is not part of the Go public API, but has been stable since Go 1.0.
type emptyInterface struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

// IsNil reports whether v is an untyped nil or an interface value holding a
// typed nil pointer (for example, (*T)(nil) stored in trace.SessionInfo).
//
// Trace hooks pass metadata through interfaces backed by pointers; this check
// is intentionally narrow and avoids reflect on the hot path.
func IsNil(v any) bool {
	if v == nil {
		return true
	}

	return (*emptyInterface)(unsafe.Pointer(&v)).data == nil
}
