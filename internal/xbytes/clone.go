package xbytes

import "bytes"

// Clone returns a copy of b[:len(b)].
// The result may have additional unused capacity.
// Clone(nil) returns nil.
func Clone(b []byte) []byte {
	return bytes.Clone(b)
}
