package xiface

import "unsafe"

// IsNil reports whether v is an untyped nil or an interface value holding a
// typed nil pointer (for example, (*T)(nil) stored in trace.SessionInfo).
//
// Trace hooks pass metadata through interfaces backed by pointers; this check
// is intentionally narrow and avoids reflect on the hot path.
func IsNil(v any) bool {
	if v == nil {
		return true
	}

	type eface struct {
		typ  unsafe.Pointer
		data unsafe.Pointer
	}

	return (*eface)(unsafe.Pointer(&v)).data == nil
}
