package xatomic

// the file contains types, copied from go 1.19 atomic/type for use in older go

import "sync/atomic"

// A Bool is an atomic boolean value.
// The zero value is false.
type Bool struct {
	_ noCopy
	v uint32
}

// Load atomically loads and returns the value stored in x.
func (x *Bool) Load() bool { return atomic.LoadUint32(&x.v) != 0 }

// Store atomically stores val into x.
func (x *Bool) Store(val bool) { atomic.StoreUint32(&x.v, b32(val)) }

// Swap atomically stores new into x and returns the previous value.
func (x *Bool) Swap(new bool) (old bool) { return atomic.SwapUint32(&x.v, b32(new)) != 0 }

// CompareAndSwap executes the compare-and-swap operation for the boolean value x.
func (x *Bool) CompareAndSwap(old, new bool) (swapped bool) {
	return atomic.CompareAndSwapUint32(&x.v, b32(old), b32(new))
}

// b32 returns a uint32 0 or 1 representing b.
func b32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

// noCopy may be added to structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
//
// Note that it must not be embedded, due to the Lock and Unlock methods.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
